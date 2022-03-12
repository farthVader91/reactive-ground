package com.vgd.rpg;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.BoundedSourceQueue;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import java.time.Duration;
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

  public static final int crawlDepthLimit = 6;
  public static final String seedUrl = "https://en.wikipedia.org/wiki/Main_Page";
  private static ActorSystem actorSystem = ActorSystem.create();
  private static Http http = Http.get(actorSystem);
  private static Pattern hrefP = Pattern.compile("href=\"(.*?)\"");

  public static void main(String[] args) {
    Source<Pair<String, Long>, NotUsed> urlSrc = Source.single(
        Pair.create(seedUrl, 1L));

    Pair<BoundedSourceQueue<Pair<String, Long>>, Source<Pair<String, Long>, NotUsed>> pair = Source.<Pair<String, Long>>queue(
            64)
        .preMaterialize(actorSystem);
    BoundedSourceQueue<Pair<String, Long>> queue = pair.first();
    Source<Pair<String, Long>, NotUsed> qSrc = pair.second();

    urlSrc.concat(qSrc)
        .log("crawling")
        .filter(uriAndDepth -> uriAndDepth.second() < 8)
        .mapAsyncUnordered(32, urlAndDepth -> Crawler.processUrl(urlAndDepth)
            .thenApply(body -> PageDescriptor.builder()
                .body(body)
                .uri(urlAndDepth.first())
                .depth(urlAndDepth.second())
                .build()))
        .alsoTo(Flow.of(PageDescriptor.class)
            .log("page-stats",
                pd -> String.format("uri: %s, num-words: %d, crawl-depth: %d", pd.getUri(), pd.getBody().length(), pd.getDepth()))
            .to(Sink.ignore()))
        .mapConcat(
            pd -> hrefP.matcher(pd.getBody())
                .results()
                .map(mr -> mr.group(1))
                .filter(href -> href.startsWith("http"))
                .map(href -> Pair.create(pd, href))
                .collect(Collectors.toList()))
        .map(pdAndHref -> Pair.create(pdAndHref.second(),
              pdAndHref.first().getDepth() + 1))
//        .log("enqueuing")
        .map(queue::offer)
        .to(Sink.ignore()).withAttributes(Attributes.createLogLevels(Logging.InfoLevel()))
        .run(actorSystem);

  }

  private static CompletionStage<String> processUrl(Pair<String, Long> urlAndDepth) {
    try {
      return http.singleRequest(HttpRequest.create().withUri(urlAndDepth.first()))
          .thenCompose(resp -> resp.entity().toStrict(512, actorSystem))
          .thenApply(resp -> resp.getData().utf8String())
          .exceptionally(err -> "");
    } catch (RuntimeException re) {
      return CompletableFuture.completedFuture("");
    }
  }

  @Value
  @Builder(toBuilder = true)
  @ToString(exclude = {"body"})
  static class PageDescriptor {
    String uri;
    Long depth;
    String body;
  }
}
