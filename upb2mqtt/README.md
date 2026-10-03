# upb2mqtt

Home Assistant app (add-on) that bridges a UPB PIM to MQTT with discovery, using
upb-lib.

Compared with Home Assistant's built-in UPB integration:

- **Restarting Home Assistant is fast and quiet.** Light state lives on the
  MQTT broker, outside Home Assistant. The integration re-reads every device
  each time Home Assistant restarts or updates: on a 65-device house that's
  more than a minute of powerline traffic, buzzing through the dimmers, before
  every light has its state again. Here Home Assistant gets every state from
  the broker the moment it's up, and devices are read only when the add-on
  itself starts.
- **Every command is as fast as your powerline network allows.** How often a
  message has to be sent depends on where the device sits on the network: one
  on the PIM's own phase hears it sent once, while one on the other phase needs
  it sent twice. The integration sends every message the same number of
  times, so if some devices need repeats, all of them pay for it. The bridge
  learns which devices answer a single transmission and sends once to those:
  about 0.35 s per command instead of 0.65 s, and 0.8 s per state read instead
  of 1.4 s. ([Transmit count](DOCS.md#transmit-count))
- **Dual PIMs, one on each phase, for even faster commands.** With a second PIM
  on the other leg of the house, the devices that needed repeats are reached
  with a single transmission too, so nearly every command takes the fast path.
  And if either PIM goes offline, the other carries on.
  ([Two PIMs](DOCS.md#two-pims))
- **Light groups switch together, with a single command.** Home Assistant
  turns a light group on or off by sending each light its own command, so the
  lights change one after another. The bridge holds those commands for a
  moment, and when a UPB scene (link) sets exactly those lights to those
  levels, it sends that one scene command instead: one powerline message
  rather than one per light, the lights change at once, and keypads showing
  the scene update their LEDs. ([Light groups](DOCS.md#light-groups))
- **Keypad LEDs stay synchronized with the lights.** A keypad toggle button
  sends "on" or "off" depending on whether its LED is lit, and a keypad
  updates its LEDs only when it hears one of its own scenes. When Home
  Assistant turns lights on or off directly, the LEDs go stale, and the next
  button press seems to do nothing. The bridge works out what each LED should
  show and corrects the ones that are wrong. ([Keypad LEDs](DOCS.md#keypad-leds))

See [DOCS.md](DOCS.md) for configuration and entities.

## Development

```sh
pip install -r requirements.txt amqtt
python tests/test_bridge.py          # fake PIM + in-process broker
python tests/test_dual_pim.py        # two fake PIMs on opposite phases
python upb2mqtt.py --pim-url socket://10.0.5.3:2101 \
    --upe-file tests/test_network.upe --mqtt-host 127.0.0.1 --log-level debug
```

`tests/test_network.upe` is a 4-device subset of the real network: Office Main and Office Fireplace (dimmers), Master Bath
Vanity (relay), and Entry KP (keypad), plus the links they belong to.
