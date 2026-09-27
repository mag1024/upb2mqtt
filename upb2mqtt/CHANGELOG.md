# Changelog

## 0.2.0

- Keypad LED sync (`keypad_sync`, on by default): keypad LEDs, and so their
  toggle state, follow the lights their scenes control, including lights
  changed directly from Home Assistant. `keypad_scene_match` chooses whether
  a multi-light scene needs `all` (default) or `any` of its lights on.
- LED states are published on `upb/keypad/<idx>/leds`.

## 0.1.2

- Fix: scenes failed to activate from Home Assistant (`KeyError: 'payload_on'`);
  discovery now sends `payload_on` explicitly.
- Add a *Refresh state* button (diagnostic) to every UPB device, the MQTT
  replacement for `homeassistant.update_entity` on the old integration's lights.

## 0.1.1

- Fix: run without s6-overlay so the app sees SUPERVISOR_TOKEN and can
  get the Mosquitto broker credentials from the Supervisor.

## 0.1.0

- Initial release: upb-lib 0.7.2 bridged to MQTT with Home Assistant discovery.
