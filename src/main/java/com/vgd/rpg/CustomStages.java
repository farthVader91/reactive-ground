package com.vgd.rpg;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.SinkShape;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractInOutHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.GraphStageLogicWithLogging;
import akka.stream.stage.TimerGraphStageLogic;
import java.time.Duration;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

public class CustomStages {

  @RequiredArgsConstructor
  static class MahSource<A> extends GraphStage<SourceShape<A>> {

    private final Outlet<A> out = Outlet.create("MahSource.out");
    private final SourceShape<A> shape = SourceShape.of(out);

    private final A seed;

    @Override
    public SourceShape<A> shape() {
      return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
      return new GraphStageLogic(shape()) {
        {
          setHandler(out, new AbstractOutHandler() {
            @Override
            public void onPull() {
              push(out, seed);
            }
          });
        }
      };
    }
  }

  static class MahSink<A> extends GraphStage<SinkShape<A>> {

    private final Inlet<A> in = Inlet.create("MahSink.in");
    private final SinkShape<A> shape = SinkShape.of(in);

    @Override
    public SinkShape<A> shape() {
      return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
      return new GraphStageLogicWithLogging(shape()) {
        @Override
        public void preStart() throws Exception, Exception {
          pull(in);
        }

        {
          setHandler(in, new AbstractInHandler() {
            @Override
            public void onPush() {
              A grab = grab(in);
              log().info("Grabbed: {}", grab);
              pull(in);
            }
          });
        }
      };
    }
  }

  @RequiredArgsConstructor
  static class MahFlo<A, B> extends GraphStage<FlowShape<A, B>> {

    private final Inlet<A> in = Inlet.create("MahFlo.in");
    private final Outlet<B> out = Outlet.create("MahFlo.out");
    private final FlowShape<A, B> shape = FlowShape.of(in, out);

    private final Function<A, B> fx;

    @Override
    public FlowShape<A, B> shape() {
      return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes)
        throws Exception, Exception {
      return new GraphStageLogicWithLogging(shape()) {
        {
          setHandlers(in, out, new AbstractInOutHandler() {
            @Override
            public void onPush() throws Exception, Exception {
              A grabbed = grab(in);
              push(out, fx.apply(grabbed));
            }

            @Override
            public void onPull() throws Exception, Exception {
              pull(in);
            }
          });
        }
      };
    }
  }

  static class TimedGate<A> extends GraphStage<FlowShape<A, A>> {

    Inlet<A> in = Inlet.create("TimedGate.in");
    Outlet<A> out = Outlet.create("TimedGate.out");
    FlowShape<A, A> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<A, A> shape() {
      return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception, Exception {
      return new TimerGraphStageLogic(shape()) {
        private boolean isOpen;

        @Override
        public void onTimer(Object timerKey) throws Exception, Exception {
          if (timerKey.equals("key")) {
            isOpen = false;
          }
        }

        {
          setHandler(in, new AbstractInHandler() {

            @Override
            public void onPush() throws Exception, Exception {
              A elem = grab(in);
              if (isOpen) {
                pull(in);
              } else {
                push(out, elem);
                isOpen = true;
                scheduleOnce("key", Duration.ofSeconds(2));
              }
            }
          });

          setHandler(out, new AbstractOutHandler() {
            @Override
            public void onPull() throws Exception, Exception {
              pull(in);
            }
          });
        }
      };
    }
  }

}
