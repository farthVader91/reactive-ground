package com.vgd.rpg;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.ClosedShape;
import akka.stream.OverflowStrategy;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Concat;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Crawler {
  public static final String seedUrl = "https://en.wikipedia.org/wiki/Main_Page";
  private static ActorSystem actorSystem = ActorSystem.create();
  private static Http http = Http.get(actorSystem);
  private static Pattern hrefP = Pattern.compile("href=\"(.*?)\"");

  public static void main(String[] args) {
    RunnableGraph<NotUsed> runnableGraph = crawlerWithSeed(new CrawlSpec(seedUrl, 1L));

    runnableGraph.withAttributes(Attributes.createLogLevels(Logging.InfoLevel()))
        .run(actorSystem);
  }


  private static CompletionStage<String> processUrl(String url) {
    try {
      return http.singleRequest(HttpRequest.create().withUri(url))
          .thenCompose(resp -> resp.entity().toStrict(1024, actorSystem))
          .thenApply(resp -> resp.getData().utf8String())
          .exceptionally(err -> "");
    } catch (RuntimeException re) {
      return CompletableFuture.completedFuture("");
    }
  }

  @SuppressWarnings("unchecked")
  private static RunnableGraph<NotUsed> crawlerWithSeed(CrawlSpec seed) {
    Source<CrawlSpec, NotUsed> seedSrc = Source.single(seed);
    Flow<CrawlSpec, PageDescriptor, NotUsed> procFlo = Flow.of(CrawlSpec.class)
        .zipWithIndex()
        .map(Pair::first)
        .throttle(1, Duration.ofMillis(100))
        .statefulMapConcat(() -> {
          Set<String> processed = new HashSet<>();

          return cs -> {
            if (processed.contains(cs.getUrl())) {
              return Collections.emptyList();
            }

            processed.add(cs.getUrl());
            return Collections.singletonList(cs);
          };
        })
        .mapAsync(32, cspec -> processUrl(cspec.getUrl())
            .thenApply(body -> PageDescriptor.builder()
                .uri(cspec.getUrl())
                .depth(cspec.getDepth())
                .body(body)
                .refs(hrefP.matcher(body)
                    .results()
                    .map(mr -> mr.group(1))
                    .filter(href -> href.startsWith("http"))
                    .collect(Collectors.toList()))
                .build()));

    Sink<PageDescriptor, NotUsed> pageSink = Flow.of(PageDescriptor.class)
        .log("page-stats",
            pd -> String.format("uri: %s, num-words: %d, crawl-depth: %d", pd.getUri(),
                pd.getBody().length(), pd.getDepth()))
        .to(Sink.ignore());

    return RunnableGraph.fromGraph(
        GraphDSL.create(b -> {
          UniformFanInShape<CrawlSpec, CrawlSpec> fanin = b.add(Concat.create());
          UniformFanOutShape<PageDescriptor, PageDescriptor> bcast = b.add(Broadcast.create(2));

          b.from(b.add(seedSrc)).toFanIn(fanin);

          b.from(fanin)
              .via(b.add(procFlo))
              .toFanOut(bcast);

          b.from(bcast).to(b.add(pageSink));
          b.from(bcast)
              .via(
                  b.add(Flow.of(PageDescriptor.class)
                      .mapConcat(pd -> pd.getRefs().stream()
                          .map(ref -> new CrawlSpec(ref, pd.getDepth() + 1))
                          .collect(Collectors.toList()))
                      .buffer(99999999, OverflowStrategy.backpressure())))
              .toFanIn(fanin);

          return ClosedShape.getInstance();
        }));
  }

  @Value
  static class CrawlSpec {

    String url;
    Long depth;
  }

  @Value
  @Builder(toBuilder = true)
  @ToString(exclude = {"body"})
  static class PageDescriptor {

    String uri;
    Long depth;
    String body;
    List<String> refs;
  }
}
