# upb2mqtt

Connects to a UPB PIM using [upb-lib](https://github.com/gwww/upb-lib), the
same library the Home Assistant UPB integration uses, and publishes devices
and links to MQTT with Home Assistant discovery.

Light states are published **retained**. When Home Assistant restarts it gets
every state from the broker right away, without re-reading every device over
the powerline. The bridge queries all devices only when the add-on itself
starts (about 1.4 s per device).

## Configuration

| Option | Description |
| --- | --- |
| `pim_url` | Serial device (`/dev/serial/by-id/usb-...`) or network PIM (`socket://10.0.5.3:2101`). |
| `upe_file` | UPStart export. Your HA config folder is mounted read-only at `/homeassistant`. |
| `refresh_hours` | Re-query all devices this often (0 = off). Commands wait behind a refresh. |
| `topic_prefix` | Default `upb`. |
| `discovery_prefix` | Default `homeassistant`. |
| `mqtt_*` | Only needed if you don't use the Mosquitto add-on. |
| `log_level` | `debug` also turns on upb-lib's protocol logging. |

upb-lib reads flags from the UPE file's `98,` record, the same way the
integration does. `tx_count=2` makes the bridge send each message twice,
which some devices need.

## Entities

- **Light**, one per device channel, with on/off, brightness, transition
  (dimmable devices only) and flash. Keypads are skipped: they have no load,
  and their button presses appear as link events.
- **Scene**, one per link. Activating it activates the link.
- **Event**, one per link (`event.upb_link_<name>`). It fires `activated`,
  `deactivated`, `goto`, `blink`, `fade_started` or `fade_stopped` with
  `brightness_pct` and `rate` attributes, whether the link was triggered
  from HA or from a keypad. This replaces the `upb.scene_changed` bus event.
- **Button** "<link> deactivate", one per link. Disabled by default.
- On the **UPB PIM** device: a *Connected* binary sensor and a *Refresh
  device states* button.

Entities become unavailable when the add-on or the PIM is disconnected.

## Replacing the `upb.*` services

Send these with `mqtt.publish`. `<idx>` is shown as the `address` attribute
on each entity, for example `1_14_0` for a light and `1_17` for a link.

| Old service | Topic | Payload |
| --- | --- | --- |
| `upb.light_fade_start` | `upb/light/<idx>/cmd` | `{"command": "fade_start", "brightness_pct": 30, "rate": 10}` |
| `upb.light_fade_stop` | `upb/light/<idx>/cmd` | `{"command": "fade_stop"}` |
| `upb.light_blink` | `upb/light/<idx>/cmd` | `{"command": "blink", "blink_rate": 0.5}` |
| `homeassistant.update_entity` | `upb/light/<idx>/cmd` | `{"command": "update_status"}` |
| `upb.link_deactivate` | `upb/link/<idx>/cmd` | `{"command": "deactivate"}` |
| `upb.link_goto` | `upb/link/<idx>/cmd` | `{"command": "goto", "brightness_pct": 60, "rate": 3}` |
| `upb.link_fade_start` | `upb/link/<idx>/cmd` | `{"command": "fade_start", "brightness_pct": 0, "rate": 30}` |
| `upb.link_fade_stop` | `upb/link/<idx>/cmd` | `{"command": "fade_stop"}` |
| `upb.link_blink` | `upb/link/<idx>/cmd` | `{"command": "blink", "blink_rate": 1}` |
| — | `upb/cmd` | `{"command": "sync"}` re-reads every device |

`brightness` (0–255) is accepted in place of `brightness_pct`. `rate` is in
seconds, or a raw UPB rate if the UPE file sets `use_raw_rate`. The default
`-1` means the device's own default rate.

```yaml
action: mqtt.publish
data:
  topic: upb/link/1_17/cmd
  payload: '{"command": "fade_start", "brightness_pct": 0, "rate": 30}'
```

## Migrating from the UPB integration

1. Delete the UPB integration (Settings → Devices & services). This frees
   its entity IDs.
2. Start the add-on. Discovery requests the same entity IDs the integration
   produced (`light.first_floor_office_main`, `scene.common_off`, ...), so
   dashboards and automations keep working.
3. Entity IDs you renamed by hand, and areas you assigned, belonged to the
   old integration's entities. Set them again on the new entities.
4. Any automation that used the `upb.scene_changed` event should use the
   link's event entity instead.

Requires Home Assistant 2025.10 or later (`default_entity_id` in discovery).

## Known limitations

This comes from upb-lib and behaves the same as in the integration.

- When a link is activated, device levels are *assumed* from the link
  table in the UPE file, not read back. `refresh_hours` or the *Refresh
  device states* button corrects any drift.
