class OrderlyCustomMetadata:
    def __init__(self):
        self.resources = []
        self.artefacts = []


class RunningOrderlyPacket:
    def __init__(self, packet, path_src):
        self.packet = packet
        self.path_src = path_src
        self.orderly = OrderlyCustomMetadata()


class ActiveOrderlyPacket:
    _packet = None

    def __init__(self, packet, path_src):
        self._our_packet = RunningOrderlyPacket(packet, path_src)

    def __enter__(self):
        ActiveOrderlyPacket._packet = self._our_packet
        return self._our_packet.orderly

    def __exit__(self, exc_type, exc_value, exc_tb):
        ActiveOrderlyPacket._packet = None

    def current():
        return ActiveOrderlyPacket._packet


def get_active_packet():
    return ActiveOrderlyPacket.current()
