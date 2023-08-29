class RunningOrderlyPacket:
    def __init__(self, packet, path_src):
        self.packet = packet
        self.path_src = path_src
        self.resources = []
        self.artefacts = []


class ActivePacket:
    _packet = None

    def __init__(self, packet, path_src):
        self._our_packet = RunningOrderlyPacket(packet, path_src)

    def __enter__(self):
        self._packet = self._our_packet

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._packet = None

    def current():
        return ActivePacket._packet


def get_active_packet():
    return ActivePacket.current()
