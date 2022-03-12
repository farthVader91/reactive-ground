package com.vgd.rpg;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.DelayOverflowStrategy;
import akka.stream.FlowShape;
import akka.stream.KillSwitches;
import akka.stream.SharedKillSwitch;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.alpakka.file.DirectoryChange;
import akka.stream.alpakka.file.javadsl.DirectoryChangesSource;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Concat;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AmzFlo {

  public static final String EOF_MARKER = "EOF";
  public static final String INPUT_DIR = "C:\\Users\\cartm\\Documents\\tmp";

  public static void main(String[] args) throws IOException {
    ActorSystem actorSystem = ActorSystem.create();
    SharedKillSwitch sks = KillSwitches.shared("sks");
    DirectoryChangesSource.create(Paths.get(INPUT_DIR), Duration.ofSeconds(1), 1024)
        .filter(pnc -> pnc.second().equals(DirectoryChange.Creation))
        .map(Pair::first)
        .via(pathToLines())
        .splitAfter(Line::isEof)
        .via(sks.flow())
        .via(proFlo(Flow.of(Line.class)
            .delay(Duration.ofSeconds(1), DelayOverflowStrategy.backpressure())
            .log("post-process")
            .via(chkPoint())
        ))
        .mergeSubstreams()
        .toMat(arcSnk(), Keep.right())
        .addAttributes(Attributes.inputBuffer(1, 1))
        .addAttributes(Attributes.createLogLevels(Logging.InfoLevel()))
        .run(actorSystem)
        .thenRun(() -> log.info("stream torn down"));

    System.in.read();
    sks.shutdown();
  }

  @Value
  static class Line {
    Path fromFile;
    Long offset;
    String content;
    boolean isEof;
  }

  private static Flow<Line, Line, NotUsed> chkPoint() {
    return Flow.of(Line.class)
        .log("checkpoint");
  }

  @SuppressWarnings("unchecked")
  private static Flow<Line, Line, NotUsed> proFlo(Flow<Line, Line, NotUsed> delegate) {
    return Flow.fromGraph(
        GraphDSL.create(delegate,
            (b, d) -> {
              UniformFanOutShape<Line, Line> bcast = b.add(Broadcast.create(2));
              UniformFanInShape<Line, Line> concat = b.add(Concat.create());

              b.from(bcast)
                  .via(b.add(Flow.of(Line.class)
                      .filterNot(Line::isEof)))
                  .via(d)
                  .toFanIn(concat);

              b.from(bcast)
                  .via(b.add(Flow.of(Line.class)
                      .filter(Line::isEof)))
                  .toFanIn(concat);

              return FlowShape.of(bcast.in(), concat.out());
            })
    );
  }

  private static Sink<Line, CompletionStage<Done>> arcSnk() {
    return Flow.of(Line.class)
        .filter(Line::isEof)
        .log("archive")
        .delay(Duration.ofMillis(500), DelayOverflowStrategy.backpressure())
        .toMat(Sink.ignore(), Keep.right());
  }

  private static Flow<Path, Line, NotUsed> pathToLines() {
    return Flow.of(Path.class)
        .flatMapConcat(path -> FileIO.fromFile(path.toFile())
            .via(Framing.delimiter(ByteString.fromString(System.lineSeparator()), 1024, FramingTruncation.ALLOW))
            .map(ByteString::utf8String)
            .zipWithIndex()
            .map(lno -> new Line(path, lno.second(), lno.first(), false))
            .concat(Source.single(new Line(path, -1L, null, true)))
        )
        .log("lines");
  }

}
