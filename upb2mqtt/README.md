# upb2mqtt

Home Assistant app (add-on) that bridges a UPB PIM to MQTT with discovery, using
upb-lib. Light state is retained on the broker, so Home Assistant restarts
don't trigger a powerline re-read of every device. See [DOCS.md](DOCS.md).

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
