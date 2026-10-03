# upb2mqtt

Home Assistant app (add-on) that bridges a Universal Powerline Bus (UPB) PIM to MQTT,
with Home Assistant discovery. It uses [upb-lib](https://github.com/gwww/upb-lib),
the library behind HA's UPB integration.

Compared with Home Assistant's built-in UPB integration:

- **Restarting Home Assistant is quiet.** Light state lives on the MQTT
  broker, outside Home Assistant. The integration re-reads every device each
  time Home Assistant restarts or updates, which on a 65-device house is
  more than a minute of powerline traffic, buzzing through the dimmers.
  Here Home Assistant gets every state from the broker at once, and devices
  are read only when the add-on itself starts.
- **Every command is as fast as the device allows.** A device on the PIM's
  own phase hears a message sent once; the rest need it sent twice. The
  integration sends every message the same number of times, so if some
  devices need repeats, all of them pay for it. The bridge learns which
  devices answer a single transmission and sends once to those: about
  0.35 s per command instead of 0.65 s, and 0.8 s per state read instead of
  1.4 s. ([Transmit count](upb2mqtt/DOCS.md#transmit-count))
- **Two PIMs, one on each phase.** With a second PIM on the other leg of the
  house, nearly every device is reached with a single transmission, and if
  either PIM goes offline the other carries on. ([Two PIMs](upb2mqtt/DOCS.md#two-pims))
- **Light groups switch together.** Home Assistant turns a light group on or
  off by sending each light its own command, so the lights change one after
  another. The bridge holds those commands for a moment, and when a UPB
  scene (link) sets exactly those lights to those levels, it sends that one
  scene command instead: the lights change at once, and keypads showing the
  scene update their LEDs. ([Light groups](upb2mqtt/DOCS.md#light-groups))
- **Keypad LEDs stay in step with the lights.** A keypad toggle button sends
  "on" or "off" depending on whether its LED is lit, and a keypad updates its
  LEDs only when it hears one of its own scenes. When Home Assistant turns
  lights on or off directly, the LEDs go stale, and the next button press
  seems to do nothing. The bridge works out what each LED should show and
  corrects the ones that are wrong. ([Keypad LEDs](upb2mqtt/DOCS.md#keypad-leds))

## Install

Settings → Apps → App store → ⋮ → Repositories, add
`https://gitea.ctl.luminal.org/mag/upb2mqtt`, then install **upb2mqtt**.

See [upb2mqtt/DOCS.md](upb2mqtt/DOCS.md) for configuration, entities, and
migrating from the UPB integration.
