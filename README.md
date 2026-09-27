# upb2mqtt

Home Assistant app (add-on) that bridges a Universal Powerline Bus (UPB) PIM to MQTT,
with Home Assistant discovery. It uses [upb-lib](https://github.com/gwww/upb-lib),
the library behind HA's UPB integration, and keeps light state retained on the
broker, so HA restarts don't re-read every device over the powerline.

## Install

Settings → Apps → App store → ⋮ → Repositories, add
`https://gitea.ctl.luminal.org/mag/upb2mqtt`, then install **upb2mqtt**.

See [upb2mqtt/DOCS.md](upb2mqtt/DOCS.md) for configuration, entities, and
migrating from the UPB integration.
