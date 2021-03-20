
package io.cloudio.scale.kafka.eventbus;

public class BusyEvent {

  enum Event {
    Busy, Free
  }

  public static final BusyEvent Busy = new BusyEvent(Event.Busy);
  public static final BusyEvent Free = new BusyEvent(Event.Free);
  private final Event event;

  public BusyEvent(Event state) {
    this.event = state;
  }

  public boolean isBusyEvent() {
    return event == Event.Busy;
  }

  public boolean isFreeEvent() {
    return event == Event.Free;
  }

  @Override
  public String toString() {
    return event.toString();
  }
}
