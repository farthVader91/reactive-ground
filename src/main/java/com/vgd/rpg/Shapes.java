package com.vgd.rpg;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.japi.function.Function2;
import akka.stream.Attributes;
import akka.stream.ClosedShape;
import akka.stream.DelayOverflowStrategy;
import akka.stream.FanInShape2;
import akka.stream.FlowShape;
import akka.stream.KillSwitches;
import akka.stream.SharedKillSwitch;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Concat;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.ZipWith;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Shapes {

  public static void main(String[] args) throws IOException {
    ActorSystem actorSystem = ActorSystem.create();
//    clo().run(actorSystem);
//    foobar().run(actorSystem);
    SharedKillSwitch sks = KillSwitches.shared("s");
    Source.range(0, 10)
        .delay(Duration.ofSeconds(1), DelayOverflowStrategy.backpressure())
        .watchTermination((n, f) -> f.whenComplete((d, t) -> {
          if (t != null) {
            log.error("issue", t);
            return;
          }

          log.info("done: {}", d);
        }))
        .log("b")
        .delay(Duration.ofSeconds(2), DelayOverflowStrategy.backpressure())
        .log("a")
        .addAttributes(Attributes.inputBuffer(1, 1))
        .addAttributes(Attributes.createLogLevels(Logging.InfoLevel()))
        .via(sks.flow())
        .runWith(Sink.ignore(), actorSystem)
        .thenRun(() -> log.info("stream terminated"));

    System.in.read();
    sks.shutdown();

  }

  private static Source<Object, NotUsed> foobar() {
    return Source.from(
            Arrays.asList(
                Arrays.asList(1, 2, 3, 4, 5),
                Arrays.asList(10, 13, 15),
                Arrays.asList(492, 301, 224, 885)
            )
        )
        .mapConcat(l -> {
          LinkedList<Integer> copy = new LinkedList<>(l);
          copy.add(-1);
          return copy;
        })
        .splitAfter(i -> i < 0)
        .via(bypass(Flow.of(Integer.class)
            .map(i -> "this is " + i)
            .log("biz")))
        .mergeSubstreams()
        .log("archive-step")
        .addAttributes(Attributes.createLogLevels(Logging.InfoLevel()))
        .addAttributes(Attributes.inputBuffer(1, 1));
  }

  private static Flow<Integer, Object, NotUsed> bypass(Flow<Integer, String, NotUsed> biz) {
    return Flow.fromGraph(
        GraphDSL.create(biz,
            (b, f) -> {
              UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));
              UniformFanInShape<Object, Object> concat = b.add(Concat.create(2));

              b.from(bcast)
                  .via(b.add(Flow.of(Integer.class)
                      .filter(i -> i > 0)))
                  .via(f)
                  .via(b.add(Flow.of(String.class)
                      .dropWhile(__ -> true)))
                  .toFanIn(concat);

              b.from(bcast)
                  .via(b.add(Flow.of(Integer.class)
                      .filter(i -> i < 0)
                  ))
                  .toFanIn(concat);

              return FlowShape.of(bcast.in(), concat.out());
            }));
  }

  private static RunnableGraph<NotUsed> clo() {
    return RunnableGraph.fromGraph(
            GraphDSL.create(b -> {
              b.from(b.add(Source.single(42)
                      .log("b")))
                  .via(b.add(Flow.of(Integer.class)
                      .map(i -> i + 2)
                      .log("a")))
                  .to(b.add(Sink.ignore()));

              return ClosedShape.getInstance();
            }))
        .withAttributes(Attributes.createLogLevels(Logging.InfoLevel()));
  }


  private static <A, T, O> Flow<A, O, NotUsed> flo(Flow<A, T, NotUsed> mainFlow,
      Function2<A, T, O> combiner) {
    return Flow.fromGraph(
        GraphDSL.create(mainFlow,
            (b, f) -> {
              UniformFanOutShape<A, A> bcast = b.add(Broadcast.create(2));
              FanInShape2<A, T, O> zip = b.add(ZipWith.create(combiner));

              b.from(bcast)
                  .via(f)
                  .toInlet(zip.in1());
              b.from(bcast).toInlet(zip.in0());

              return FlowShape.of(bcast.in(), zip.out());
            })
    );
  }

}
