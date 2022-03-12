package com.vgd.rpg;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.CoordinatedShutdown;
import akka.event.Logging;
import akka.japi.Pair;
import akka.stream.ActorAttributes;
import akka.stream.Attributes;
import akka.stream.DelayOverflowStrategy;
import akka.stream.Inlet;
import akka.stream.KillSwitches;
import akka.stream.SinkShape;
import akka.stream.Supervision;
import akka.stream.Supervision.Directive;
import akka.stream.UniqueKillSwitch;
import akka.stream.alpakka.file.javadsl.FileTailSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import com.vgd.rpg.CustomStages.TimedGate;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {


  private static Supervision.Directive decider(Throwable throwable) {
    if (throwable instanceof TimeoutException) {
      return (Directive) Supervision.stop();
    } else {
      return (Directive) Supervision.resume();
    }
  }

  static class CustomSink extends GraphStage<SinkShape<Integer>> {

    private final Inlet<Integer> in = Inlet.create("CustomSink.in");
    private final SinkShape<Integer> shape = SinkShape.of(in);

    @Override
    public SinkShape<Integer> shape() {
      return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception, Exception {
      return new GraphStageLogic(shape()) {
        @Override
        public void preStart() throws Exception, Exception {
          pull(in);
        }

        {
          setHandler(in, new AbstractInHandler() {
            @Override
            public void onPush() {
              Integer grabbed = grab(in);
              log.info("grabbed: {}", grabbed);
              pull(in);
            }
          });
        }
      };
    }
  }

  public static void main(String[] args) throws IOException {
    ActorSystem actorSystem = ActorSystem.create();

    Source.range(0, 1_000)
        .delay(Duration.ofMillis(500), DelayOverflowStrategy.backpressure())
        .via(Flow.fromGraph(new TimedGate<>()))
        .log("ele").withAttributes(Attributes.createLogLevels(Logging.InfoLevel()))
        .run(actorSystem);

    System.in.read();

  }

  private static void stuff(String[] args, ActorSystem actorSystem) {

    Source<String, NotUsed> src = FileTailSource.createLines(
            Paths.get(args[0]),
            1024,
            Duration.ofMillis(1000),
            "\n",
            StandardCharsets.UTF_8)
        .log("filetail");

    Pair<UniqueKillSwitch, CompletionStage<Done>> pair = src
        .idleTimeout(Duration.ofSeconds(1))
        .recoverWithRetries(3, Throwable.class, () -> src)
        .viaMat(KillSwitches.single(), Keep.right())
        .toMat(Sink.ignore(), Keep.both())
        .withAttributes(ActorAttributes.withSupervisionStrategy(Main::decider))
        .withAttributes(Attributes.createLogLevels(Attributes.logLevelInfo()))
        .run(actorSystem);
    UniqueKillSwitch ks = pair.first();
    pair.second().whenComplete((done, thr) -> {
      log.info("{} - {}", done, thr);
    });
    pair.second().thenRun(() -> {
      log.info("stream was torn down properly.");
    });

    ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
    pool.schedule(ks::shutdown, 100000, TimeUnit.MILLISECONDS);

    CoordinatedShutdown.get(actorSystem)
        .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate(),
            "teardown-stream", () -> {
              ks.shutdown();
              return CompletableFuture.completedFuture(Done.getInstance());
            });

  }

  public static void oldMain(String[] args) throws IOException, InterruptedException {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    CountDownLatch latch = new CountDownLatch(1);
    try (AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(
        Paths.get(args[0]),
        StandardOpenOption.READ)) {
      AtomicInteger bytesRead = new AtomicInteger(1);
      do {
        fileChannel.read(buffer, bytesRead.get(), buffer, new CompletionHandler<>() {
          @Override
          public void completed(Integer result, ByteBuffer attachment) {
            log.info("bytes read: {}", result);
            bytesRead.set(result);
            if (result == 0) {
              latch.countDown();
              return;
            }
            log.info(new String(attachment.array(), StandardCharsets.UTF_8));
            attachment.clear();
          }

          @Override
          public void failed(Throwable exc, ByteBuffer attachment) {
            log.error("error", exc);
            latch.countDown();

          }
        });
      } while (bytesRead.get() > 0);
    }

    latch.await();
//    Flux<String> linesFlux = Flux.create(
//        sink -> fileChannel.read(buffer, 0, buffer, new CompletionHandler<>() {
//          @Override
//          public void completed(Integer result, ByteBuffer attachment) {
//            if (result == 0) {
//              sink.complete();
//              return;
//            }
//            sink.next(new String(attachment.array(), StandardCharsets.UTF_8));
//          }
//
//          @Override
//          public void failed(Throwable exc, ByteBuffer attachment) {
//            sink.error(exc);
//          }
//        }));
//    linesFlux
//        .map(String::toUpperCase)
//        .log()
//        .blockLast();

//    Flux.interval(Duration.ofMillis(20))
//        .take(10, true)
//        .log("before")
//        .delayElements(Duration.ofMillis(25))
//        .log("after")
//        .blockLast();
  }
}
