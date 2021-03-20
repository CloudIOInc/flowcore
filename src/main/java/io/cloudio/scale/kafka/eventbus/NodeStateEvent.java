
package io.cloudio.scale.kafka.eventbus;

public class NodeStateEvent {

  enum Event {
    Pause, Resume, Shutdown
  }

  public static final NodeStateEvent Pause = new NodeStateEvent(Event.Pause);
  public static final NodeStateEvent Resume = new NodeStateEvent(Event.Resume);
  public static final NodeStateEvent Shutdown = new NodeStateEvent(Event.Shutdown);
  private final Event event;

  public NodeStateEvent(Event state) {
    this.event = state;
  }

  public boolean isPauseEvent() {
    return event == Event.Pause;
  }

  public boolean isResumeEvent() {
    return event == Event.Resume;
  }

  public boolean isShutdownEvent() {
    return event == Event.Shutdown;
  }

  @Override
  public String toString() {
    return event.toString();
  }
}
