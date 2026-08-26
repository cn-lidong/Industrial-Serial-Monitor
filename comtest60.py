"""
工业串口监控系统 v6.0 - 完整企业版
功能模块：
1. 串口 (多通道/自动重连)
2. Modbus RTU (03/06/16) - 串口版
3. Modbus TCP (03/06) - 网络版
4. CAN (收发)
5. CANopen (NMT/SDO)
6. MQTT (发布/订阅)
7. OPC UA (客户端读写) - 可选
8. 实时数据曲线 (PyQtChart)
9. 脚本自动化 (Python 引擎)
10. 多设备轮询 (定时轮询)
"""

import sys
import os
import json
import time
import threading
import queue
import struct
import subprocess
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Callable
from datetime import datetime

import serial
import serial.tools.list_ports

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

# ============================================
# PyQtChart - 可选
# ============================================
CHART_AVAILABLE = False
try:
    from PyQt5.QtChart import *
    CHART_AVAILABLE = True
except Exception:
    CHART_AVAILABLE = False

# ============================================
# 可选模块导入
# ============================================
CAN_AVAILABLE = False
try:
    import can
    CAN_AVAILABLE = True
except Exception:
    CAN_AVAILABLE = False

MQTT_AVAILABLE = False
try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except Exception:
    MQTT_AVAILABLE = False

MODBUS_AVAILABLE = False
try:
    from pymodbus.client import ModbusTcpClient
    MODBUS_AVAILABLE = True
except Exception:
    MODBUS_AVAILABLE = False

OPCUA_AVAILABLE = False
try:
    from asyncua import Client
    OPCUA_AVAILABLE = True
except Exception:
    OPCUA_AVAILABLE = False


# ============================================
# 配置管理器
# ============================================
CONFIG_FILE = "serial_v6_config.json"
AUDIT_FILE = "audit_v6.log"
LOG_DIR = "logs"


class ConfigManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._load()
            return cls._instance

    def _load(self):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        except:
            self.data = {
                "hex_mode": False,
                "log_enabled": True,
                "show_timestamp": True,
                "auto_connect": False,
                "window_x": 100,
                "window_y": 100,
                "window_width": 1400,
                "window_height": 900,
                "polling_interval": 1000,
                "modbus_rtu": {"port": "", "baud": 9600, "timeout": 1},
                "modbus_tcp": {"host": "127.0.0.1", "port": 502},
                "canopen": {"node_id": 1},
                "opcua": {"url": "opc.tcp://localhost:4840"},
                "script_path": "",
                "script_enabled": False,
            }

    def save(self):
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
        except:
            pass

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.data[key] = value
        self.save()


# ============================================
# 数据总线
# ============================================
@dataclass
class DataPacket:
    timestamp: float
    source: str
    source_type: str
    data: bytes
    value: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataBus(QObject):
    data_received = pyqtSignal(DataPacket)
    device_status = pyqtSignal(str, bool, str)

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                QObject.__init__(cls._instance)
                cls._instance._initialized = True
                cls._instance._subscribers = defaultdict(list)
                cls._instance._history = deque(maxlen=10000)
            return cls._instance

    def publish(self, packet: DataPacket):
        self._history.append(packet)
        self.data_received.emit(packet)
        for key, cbs in list(self._subscribers.items()):
            if key == packet.source_type or key == "*":
                for cb in cbs:
                    try:
                        cb(packet)
                    except:
                        pass

    def subscribe(self, source_type: str, callback: Callable):
        self._subscribers[source_type].append(callback)

    def get_history(self, limit=100):
        return list(self._history)[-limit:]


def get_data_bus():
    return DataBus.instance()


# ============================================
# Modbus RTU 协议实现
# ============================================
class ModbusRTU:
    FUNC_READ_HOLDING = 0x03
    FUNC_WRITE_SINGLE = 0x06
    FUNC_WRITE_MULTIPLE = 0x10

    @classmethod
    def _crc16(cls, data: bytes) -> bytes:
        crc = 0xFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xA001
                else:
                    crc >>= 1
        return struct.pack('<H', crc)

    @classmethod
    def build_read_holding(cls, slave: int, address: int, count: int) -> bytes:
        req = struct.pack('>BBHH', slave, cls.FUNC_READ_HOLDING, address, count)
        return req + cls._crc16(req)

    @classmethod
    def build_write_single(cls, slave: int, address: int, value: int) -> bytes:
        req = struct.pack('>BBHH', slave, cls.FUNC_WRITE_SINGLE, address, value)
        return req + cls._crc16(req)

    @classmethod
    def build_write_multiple(cls, slave: int, address: int, values: List[int]) -> bytes:
        byte_count = len(values) * 2
        req = struct.pack('>BBHHB', slave, cls.FUNC_WRITE_MULTIPLE, address, len(values), byte_count)
        for v in values:
            req += struct.pack('>H', v)
        return req + cls._crc16(req)

    @classmethod
    def parse_response(cls, data: bytes) -> Dict:
        if len(data) < 3:
            return {"error": "数据太短"}
        result = {"slave": data[0], "func": data[1], "raw": data.hex(' ').upper()}
        if len(data) >= 3:
            crc_recv = struct.unpack('<H', data[-2:])[0]
            crc_calc = struct.unpack('<H', cls._crc16(data[:-2]))[0]
            result["crc_ok"] = (crc_recv == crc_calc)
            if not result["crc_ok"]:
                result["error"] = f"CRC错误: 收到{crc_recv:04X}, 计算{crc_calc:04X}"
                return result
        if data[1] & 0x80:
            result["error"] = f"异常码: {data[2]}"
            return result
        if data[1] == cls.FUNC_READ_HOLDING:
            byte_count = data[2]
            result["values"] = []
            for i in range(0, byte_count, 2):
                if 3 + i + 1 < len(data):
                    result["values"].append((data[3 + i] << 8) | data[3 + i + 1])
        elif data[1] == cls.FUNC_WRITE_SINGLE:
            result["address"] = (data[2] << 8) | data[3]
            result["value"] = (data[4] << 8) | data[5]
        elif data[1] == cls.FUNC_WRITE_MULTIPLE:
            result["address"] = (data[2] << 8) | data[3]
            result["count"] = (data[4] << 8) | data[5]
        return result


# ============================================
# CANopen 协议 (基础)
# ============================================
class CANopen:
    NMT_START = 0x01
    NMT_STOP = 0x02
    NMT_PREOP = 0x80
    NMT_RESET = 0x81
    NMT_RESET_COMM = 0x82

    @classmethod
    def build_nmt(cls, node_id: int, command: int) -> bytes:
        return bytes([command, node_id])

    @classmethod
    def build_sdo_read(cls, node_id: int, index: int, subindex: int) -> bytes:
        cob_id = 0x600 + node_id
        data = bytes([0x40, index & 0xFF, (index >> 8) & 0xFF, subindex, 0, 0, 0, 0])
        return cob_id.to_bytes(2, 'big') + data

    @classmethod
    def build_sdo_write(cls, node_id: int, index: int, subindex: int, value: int) -> bytes:
        cob_id = 0x600 + node_id
        data = bytes([
            0x23, index & 0xFF, (index >> 8) & 0xFF, subindex,
            value & 0xFF, (value >> 8) & 0xFF, (value >> 16) & 0xFF, (value >> 24) & 0xFF
        ])
        return cob_id.to_bytes(2, 'big') + data

    @classmethod
    def parse_sdo(cls, data: bytes) -> Dict:
        if len(data) < 8:
            return {"error": "数据太短"}
        result = {
            "cob_id": (data[0] << 8) | data[1],
            "cmd": data[2],
            "index": (data[4] << 8) | data[3],
            "subindex": data[5]
        }
        if data[2] == 0x43:
            result["value"] = (data[9] << 24) | (data[8] << 16) | (data[7] << 8) | data[6]
        elif data[2] == 0x4F:
            result["value"] = data[6]
        elif data[2] == 0x5F:
            result["value"] = (data[7] << 8) | data[6]
        elif data[2] == 0x60:
            result["success"] = True
        return result


# ============================================
# 串口管理器
# ============================================
class SerialWorker(QThread):
    def __init__(self, port_id: str, port: str, baud: int):
        super().__init__()
        self.port_id = port_id
        self.port = port
        self.baud = baud
        self.serial = None
        self._running = False
        self.send_queue = deque()

    def run(self):
        try:
            self.serial = serial.Serial(self.port, self.baud, timeout=0.1)
            self._running = True
            get_data_bus().device_status.emit(self.port_id, True, f"已连接 {self.port} @ {self.baud}")

            while self._running:
                if self.send_queue:
                    data = self.send_queue.popleft()
                    self.serial.write(data)
                if self.serial.in_waiting > 0:
                    data = self.serial.read(self.serial.in_waiting)
                    if data:
                        packet = DataPacket(
                            timestamp=time.time(),
                            source=self.port_id,
                            source_type="serial",
                            data=data,
                            metadata={"port": self.port, "baud": self.baud}
                        )
                        get_data_bus().publish(packet)
                self.msleep(5)
        except Exception as e:
            get_data_bus().device_status.emit(self.port_id, False, str(e))
            self._running = False

    def send(self, data: bytes):
        self.send_queue.append(data)

    def stop(self):
        self._running = False
        if self.serial:
            try:
                self.serial.close()
            except:
                pass
        self.wait()
        get_data_bus().device_status.emit(self.port_id, False, "已断开")


class SerialManager:
    def __init__(self):
        self.workers = {}
        self._counter = 0

    def add_port(self, port: str, baud: int) -> str:
        self._counter += 1
        port_id = f"serial_{self._counter}"
        worker = SerialWorker(port_id, port, baud)
        worker.start()
        self.workers[port_id] = worker
        return port_id

    def remove_port(self, port_id: str):
        if port_id in self.workers:
            self.workers[port_id].stop()
            del self.workers[port_id]

    def remove_all(self):
        for pid in list(self.workers.keys()):
            self.remove_port(pid)

    def send_to(self, port_id: str, data: bytes):
        if port_id in self.workers:
            self.workers[port_id].send(data)

    def broadcast(self, data: bytes):
        for w in self.workers.values():
            w.send(data)

    def get_count(self):
        return len(self.workers)

    def get_ports(self):
        return [(pid, w.port, w.baud, w.isRunning()) for pid, w in self.workers.items()]


# ============================================
# Modbus TCP 管理器
# ============================================
class ModbusTCPManager(QThread):
    def __init__(self, host: str = "127.0.0.1", port: int = 502):
        super().__init__()
        self.host = host
        self.port = port
        self.client = None
        self._running = False
        self._connected = False

    def run(self):
        self._running = True
        while self._running:
            try:
                if not self._connected and MODBUS_AVAILABLE:
                    self.client = ModbusTcpClient(self.host, self.port)
                    self._connected = self.client.connect()
                    if self._connected:
                        get_data_bus().device_status.emit("modbus_tcp", True, f"已连接 {self.host}:{self.port}")
                    else:
                        get_data_bus().device_status.emit("modbus_tcp", False, "连接失败")
                self.msleep(1000)
            except Exception as e:
                get_data_bus().device_status.emit("modbus_tcp", False, str(e))
                self._connected = False
                self.msleep(5000)

    def read_registers(self, address: int, count: int, slave: int = 1):
        if self._connected and self.client:
            return self.client.read_holding_registers(address, count, slave=slave)

    def write_register(self, address: int, value: int, slave: int = 1):
        if self._connected and self.client:
            return self.client.write_register(address, value, slave=slave)

    def stop(self):
        self._running = False
        if self.client:
            try:
                self.client.close()
            except:
                pass
        get_data_bus().device_status.emit("modbus_tcp", False, "已断开")


# ============================================
# Modbus RTU 管理器 (串口)
# ============================================
class ModbusRTUManager(QThread):
    def __init__(self, port: str, baud: int, slave: int = 1, timeout: float = 1.0):
        super().__init__()
        self.port = port
        self.baud = baud
        self.slave = slave
        self.timeout = timeout
        self.serial = None
        self._running = False
        self._connected = False
        self.send_queue = deque()

    def run(self):
        self._running = True
        try:
            self.serial = serial.Serial(self.port, self.baud, timeout=self.timeout)
            self._connected = True
            get_data_bus().device_status.emit("modbus_rtu", True, f"已连接 {self.port} @ {self.baud}")
        except Exception as e:
            get_data_bus().device_status.emit("modbus_rtu", False, str(e))
            return

        while self._running:
            try:
                if self.send_queue:
                    data = self.send_queue.popleft()
                    self.serial.write(data)
                    response = self.serial.read(256)
                    if response:
                        result = ModbusRTU.parse_response(response)
                        packet = DataPacket(
                            timestamp=time.time(),
                            source="modbus_rtu",
                            source_type="modbus_rtu",
                            data=response,
                            metadata=result
                        )
                        get_data_bus().publish(packet)
                self.msleep(10)
            except Exception as e:
                get_data_bus().device_status.emit("modbus_rtu", False, str(e))
                self._connected = False
                self.msleep(5000)

    def read_holding(self, address: int, count: int):
        if self._connected and self.serial:
            req = ModbusRTU.build_read_holding(self.slave, address, count)
            self.send_queue.append(req)
            return True
        return False

    def write_single(self, address: int, value: int):
        if self._connected and self.serial:
            req = ModbusRTU.build_write_single(self.slave, address, value)
            self.send_queue.append(req)
            return True
        return False

    def stop(self):
        self._running = False
        if self.serial:
            try:
                self.serial.close()
            except:
                pass
        get_data_bus().device_status.emit("modbus_rtu", False, "已断开")


# ============================================
# CAN 管理器 (修复 virtual 接口兼容性)
# ============================================
class CANManager(QThread):
    def __init__(self, interface: str = "virtual", channel: str = "vcan0", bitrate: int = 500000):
        super().__init__()
        self.interface = interface
        self.channel = channel
        self.bitrate = bitrate
        self.bus = None
        self._running = False
        self._connected = False

    def _create_bus(self):
        if self.interface == "virtual":
            try:
                return can.interface.Bus(channel=self.channel, interface="virtual")
            except Exception:
                try:
                    from can.interfaces.virtual import Bus as VirtualBus
                    return VirtualBus(channel=self.channel)
                except ImportError:
                    try:
                        subprocess.run(['ip', 'link', 'add', 'dev', 'vcan0', 'type', 'vcan'],
                                      check=False, capture_output=True)
                        subprocess.run(['ip', 'link', 'set', 'up', 'vcan0'],
                                      check=False, capture_output=True)
                        return can.interface.Bus(channel='vcan0', interface='socketcan')
                    except:
                        raise Exception("无法创建CAN接口")
        else:
            return can.interface.Bus(channel=self.channel, interface=self.interface, bitrate=self.bitrate)

    def run(self):
        self._running = True
        if not CAN_AVAILABLE:
            get_data_bus().device_status.emit("can", False, "python-can未安装")
            return

        while self._running:
            try:
                if not self._connected:
                    self.bus = self._create_bus()
                    self._connected = True
                    get_data_bus().device_status.emit("can", True, f"CAN已连接: {self.channel}")

                msg = self.bus.recv(timeout=0.1)
                if msg:
                    packet = DataPacket(
                        timestamp=time.time(),
                        source="can",
                        source_type="can",
                        data=msg.data,
                        metadata={"arbitration_id": msg.arbitration_id, "dlc": msg.dlc}
                    )
                    get_data_bus().publish(packet)
                self.msleep(5)
            except Exception as e:
                get_data_bus().device_status.emit("can", False, str(e))
                self._connected = False
                self.msleep(5000)

    def send(self, arbitration_id: int, data: bytes):
        if self._connected and self.bus:
            msg = can.Message(arbitration_id=arbitration_id, data=data)
            self.bus.send(msg)
            return True
        return False

    def stop(self):
        self._running = False
        if self.bus:
            try:
                self.bus.shutdown()
            except:
                pass
        get_data_bus().device_status.emit("can", False, "已断开")


# ============================================
# CANopen 管理器
# ============================================
class CANopenManager(QObject):
    def __init__(self, can_manager: CANManager):
        super().__init__()
        self.can = can_manager
        self.node_id = 1
        self._listener = None

    def set_node_id(self, node_id: int):
        self.node_id = node_id

    def nmt_start(self):
        data = CANopen.build_nmt(0, CANopen.NMT_START)
        self.can.send(0x000, data)

    def nmt_stop(self):
        data = CANopen.build_nmt(0, CANopen.NMT_STOP)
        self.can.send(0x000, data)

    def sdo_read(self, index: int, subindex: int):
        data = CANopen.build_sdo_read(self.node_id, index, subindex)
        self.can.send(0x600 + self.node_id, data)

    def sdo_write(self, index: int, subindex: int, value: int):
        data = CANopen.build_sdo_write(self.node_id, index, subindex, value)
        self.can.send(0x600 + self.node_id, data)


# ============================================
# MQTT 管理器 (兼容 2.x)
# ============================================
class MQTTManager(QThread):
    def __init__(self, broker: str = "localhost", port: int = 1883, client_id: str = "serial_monitor"):
        super().__init__()
        self.broker = broker
        self.port = port
        self.client_id = client_id
        self.client = None
        self._running = False
        self._connected = False
        self.publish_queue = queue.Queue()

    def run(self):
        self._running = True
        if not MQTT_AVAILABLE:
            get_data_bus().device_status.emit("mqtt", False, "paho-mqtt未安装")
            return

        while self._running:
            try:
                if not self._connected:
                    try:
                        self.client = mqtt.Client(
                            client_id=self.client_id,
                            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
                        )
                    except AttributeError:
                        self.client = mqtt.Client(client_id=self.client_id)

                    self.client.on_connect = self._on_connect
                    self.client.on_message = self._on_message
                    self.client.connect(self.broker, self.port, 60)
                    self.client.loop_start()
                    self._connected = True
                    get_data_bus().device_status.emit("mqtt", True, f"已连接 {self.broker}:{self.port}")

                while not self.publish_queue.empty():
                    topic, payload = self.publish_queue.get()
                    if self.client:
                        self.client.publish(topic, payload)

                self.msleep(100)
            except Exception as e:
                get_data_bus().device_status.emit("mqtt", False, str(e))
                self._connected = False
                self.msleep(5000)

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            get_data_bus().device_status.emit("mqtt", True, "MQTT已连接")
        else:
            get_data_bus().device_status.emit("mqtt", False, f"连接失败: {rc}")

    def _on_message(self, client, userdata, msg):
        packet = DataPacket(
            timestamp=time.time(),
            source="mqtt",
            source_type="mqtt",
            data=msg.payload,
            metadata={"topic": msg.topic}
        )
        get_data_bus().publish(packet)

    def publish(self, topic: str, payload: str):
        self.publish_queue.put((topic, payload))

    def stop(self):
        self._running = False
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except:
                pass
        get_data_bus().device_status.emit("mqtt", False, "已断开")


# ============================================
# OPC UA 管理器
# ============================================
class OPCUAManager(QThread):
    def __init__(self, url: str = "opc.tcp://localhost:4840"):
        super().__init__()
        self.url = url
        self.client = None
        self._running = False
        self._connected = False

    def run(self):
        self._running = True
        if not OPCUA_AVAILABLE:
            get_data_bus().device_status.emit("opcua", False, "opcua-asyncio未安装")
            return

        while self._running:
            try:
                if not self._connected:
                    self.client = Client(self.url)
                    self.client.connect()
                    self._connected = True
                    get_data_bus().device_status.emit("opcua", True, f"已连接 {self.url}")
                self.msleep(1000)
            except Exception as e:
                get_data_bus().device_status.emit("opcua", False, str(e))
                self._connected = False
                self.msleep(5000)

    def read_node(self, node_id: str):
        if self._connected and self.client:
            try:
                node = self.client.get_node(node_id)
                return node.get_value()
            except Exception as e:
                return {"error": str(e)}
        return None

    def write_node(self, node_id: str, value):
        if self._connected and self.client:
            try:
                node = self.client.get_node(node_id)
                node.set_value(value)
                return True
            except Exception as e:
                return {"error": str(e)}
        return False

    def stop(self):
        self._running = False
        if self.client:
            try:
                self.client.disconnect()
            except:
                pass
        get_data_bus().device_status.emit("opcua", False, "已断开")


# ============================================
# 脚本引擎
# ============================================
class ScriptEngine(QObject):
    script_executed = pyqtSignal(str, object)
    error_occurred = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.scripts = {}
        self._running = False
        self._timer = None

    def load_script(self, name: str, code: str):
        try:
            local_scope = {
                'datetime': datetime,
                'time': time,
                'print': self._print_redirect,
                'send_data': self._send_data,
                'log': self._log,
            }
            exec(code, local_scope)
            self.scripts[name] = local_scope
            self.script_executed.emit("load", f"脚本 {name} 加载成功")
            return True
        except Exception as e:
            self.error_occurred.emit(f"加载脚本失败: {e}")
            return False

    def execute_function(self, script_name: str, func_name: str, *args):
        if script_name in self.scripts:
            script = self.scripts[script_name]
            if func_name in script:
                try:
                    result = script[func_name](*args)
                    self.script_executed.emit(func_name, result)
                    return result
                except Exception as e:
                    self.error_occurred.emit(f"执行失败: {e}")
        return None

    def _print_redirect(self, *args):
        self.script_executed.emit("print", " ".join(str(a) for a in args))

    def _send_data(self, target: str, data: bytes):
        packet = DataPacket(
            timestamp=time.time(),
            source="script",
            source_type="script",
            data=data,
            metadata={"target": target}
        )
        get_data_bus().publish(packet)
        return True

    def _log(self, msg: str):
        self.script_executed.emit("log", msg)

    def start_timer(self, interval_ms: int = 1000):
        if self._timer is None:
            self._timer = QTimer()
            self._timer.timeout.connect(self._on_timer)
            self._timer.start(interval_ms)
            self._running = True

    def _on_timer(self):
        for name, script in self.scripts.items():
            if "on_timer" in script:
                try:
                    script["on_timer"]()
                except Exception as e:
                    self.error_occurred.emit(f"定时器执行失败: {e}")

    def stop_timer(self):
        if self._timer:
            self._timer.stop()
            self._timer = None
            self._running = False


# ============================================
# 轮询引擎
# ============================================
class PollingTask:
    def __init__(self, name: str, source: str, interval: int, action: str, params: Dict):
        self.name = name
        self.source = source
        self.interval = interval
        self.action = action
        self.params = params
        self.last_run = 0
        self.enabled = True


class PollingEngine(QThread):
    task_completed = pyqtSignal(str, object)

    def __init__(self):
        super().__init__()
        self.tasks = []
        self._running = False
        self._lock = threading.Lock()

    def add_task(self, task: PollingTask):
        with self._lock:
            self.tasks.append(task)

    def remove_task(self, name: str):
        with self._lock:
            self.tasks = [t for t in self.tasks if t.name != name]

    def run(self):
        self._running = True
        while self._running:
            now = time.time() * 1000
            with self._lock:
                for task in self.tasks:
                    if not task.enabled:
                        continue
                    if now - task.last_run >= task.interval:
                        task.last_run = now
                        self._execute_task(task)
            self.msleep(100)

    def _execute_task(self, task: PollingTask):
        try:
            if task.action == "modbus_read":
                slave = task.params.get("slave", 1)
                addr = task.params.get("address", 0)
                count = task.params.get("count", 10)
                req = ModbusRTU.build_read_holding(slave, addr, count)
                packet = DataPacket(
                    timestamp=time.time(),
                    source=task.source,
                    source_type="polling",
                    data=req,
                    metadata={"task": task.name}
                )
                get_data_bus().publish(packet)
                self.task_completed.emit(task.name, {"status": "sent"})
        except Exception as e:
            self.task_completed.emit(task.name, {"error": str(e)})

    def stop(self):
        self._running = False
        self.wait()


# ============================================
# 主窗口
# ============================================
class IndustrialSerialV6(QMainWindow):
    def __init__(self):
        super().__init__()
        self.config = ConfigManager()
        self.data_bus = get_data_bus()
        self.serial_manager = SerialManager()
        self.modbus_tcp = None
        self.modbus_rtu = None
        self.can_manager = None
        self.canopen_manager = None
        self.mqtt_manager = None
        self.opcua_manager = None
        self.polling_engine = PollingEngine()
        self.script_engine = ScriptEngine()
        self.chart_series = {}

        self.rx_count = 0
        self.tx_count = 0
        self.polling_tasks = []

        self._init_ui()
        self._setup_menu()
        self._setup_toolbar()
        self._setup_statusbar()
        self._setup_connections()
        self._load_config()

        self._refresh_ports()

        self._log_system("🏭 工业串口监控系统 v6.0 启动")

    # ---------- UI ----------
    def _init_ui(self):
        self.setWindowTitle("🏭 工业串口监控系统 v6.0 - 完整企业版")
        self.setMinimumSize(1400, 900)

        x = self.config.get("window_x", 100)
        y = self.config.get("window_y", 100)
        w = self.config.get("window_width", 1400)
        h = self.config.get("window_height", 900)
        self.setGeometry(x, y, w, h)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setSpacing(2)
        layout.setContentsMargins(2, 2, 2, 2)

        # 主分割器
        self.main_splitter = QSplitter(Qt.Horizontal)

        # 左侧：数据显示
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self.display_area = QTextEdit()
        self.display_area.setReadOnly(True)
        self.display_area.setFont(QFont("Consolas", 10))
        self.display_area.setStyleSheet("background:#0a0a0a; color:#00ff41; border:2px solid #333; border-radius:4px;")
        left_layout.addWidget(self.display_area)

        self.main_splitter.addWidget(left)

        # 右侧：Tab面板
        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.tabs = QTabWidget()
        self.tabs.setFont(QFont("Consolas", 10))
        self.tabs.setStyleSheet("QTabWidget::pane{border:1px solid #444;background:#1a1a1a;}")

        self.tabs.addTab(self._create_serial_tab(), "🔌 串口")
        self.tabs.addTab(self._create_modbus_tab(), "📶 Modbus")
        self.tabs.addTab(self._create_can_tab(), "🚗 CAN/CANopen")
        self.tabs.addTab(self._create_mqtt_tab(), "📡 MQTT")
        self.tabs.addTab(self._create_opcua_tab(), "🔗 OPC UA")
        self.tabs.addTab(self._create_chart_tab(), "📊 曲线")
        self.tabs.addTab(self._create_auto_tab(), "🤖 自动化")
        self.tabs.addTab(self._create_send_tab(), "📤 发送")
        self.tabs.addTab(self._create_settings_tab(), "⚙ 设置")

        right_layout.addWidget(self.tabs)
        self.main_splitter.addWidget(right)

        self.main_splitter.setSizes([450, 550])
        layout.addWidget(self.main_splitter)

        # 启动轮询引擎
        self.polling_engine.start()

    # ---------- 串口 Tab ----------
    def _create_serial_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        self.serial_table = QTableWidget()
        self.serial_table.setColumnCount(4)
        self.serial_table.setHorizontalHeaderLabels(["ID", "端口", "波特率", "状态"])
        self.serial_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.serial_table.setStyleSheet("background:#0a0a0a; color:#00ff41;")
        layout.addWidget(self.serial_table)

        ctrl = QHBoxLayout()
        self.port_combo = QComboBox()
        self.port_combo.setMinimumWidth(150)
        self.baud_combo = QComboBox()
        self.baud_combo.addItems(["9600", "19200", "38400", "57600", "115200", "230400"])
        self.baud_combo.setCurrentText("115200")
        self.btn_add = QPushButton("➕ 添加")
        self.btn_add.clicked.connect(self._add_serial)
        self.btn_refresh = QPushButton("🔄 刷新")
        self.btn_refresh.clicked.connect(self._refresh_ports)

        ctrl.addWidget(QLabel("端口:"))
        ctrl.addWidget(self.port_combo)
        ctrl.addWidget(QLabel("波特率:"))
        ctrl.addWidget(self.baud_combo)
        ctrl.addStretch()
        ctrl.addWidget(self.btn_add)
        ctrl.addWidget(self.btn_refresh)
        layout.addLayout(ctrl)

        return widget

    # ---------- Modbus Tab ----------
    def _create_modbus_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # 模式切换
        mode_layout = QHBoxLayout()
        self.mb_mode = QComboBox()
        self.mb_mode.addItems(["Modbus RTU (串口)", "Modbus TCP"])
        self.mb_mode.currentIndexChanged.connect(self._mb_mode_changed)
        mode_layout.addWidget(QLabel("模式:"))
        mode_layout.addWidget(self.mb_mode)
        mode_layout.addStretch()
        layout.addLayout(mode_layout)

        # RTU 配置
        self.mb_rtu_group = QGroupBox("Modbus RTU")
        rtu_layout = QHBoxLayout()
        self.mb_rtu_port = QComboBox()
        self.mb_rtu_port.setMinimumWidth(120)
        self.mb_rtu_baud = QComboBox()
        self.mb_rtu_baud.addItems(["9600", "19200", "38400", "57600", "115200"])
        self.mb_rtu_baud.setCurrentText("9600")
        self.mb_rtu_slave = QSpinBox()
        self.mb_rtu_slave.setRange(1, 247)
        self.mb_rtu_slave.setValue(1)
        self.btn_rtu_connect = QPushButton("连接")
        self.btn_rtu_connect.clicked.connect(self._toggle_rtu)
        self.mb_rtu_status = QLabel("⚪ 未连接")
        self.mb_rtu_status.setStyleSheet("color:#aaa;")
        rtu_layout.addWidget(QLabel("端口:"))
        rtu_layout.addWidget(self.mb_rtu_port)
        rtu_layout.addWidget(QLabel("波特率:"))
        rtu_layout.addWidget(self.mb_rtu_baud)
        rtu_layout.addWidget(QLabel("从站:"))
        rtu_layout.addWidget(self.mb_rtu_slave)
        rtu_layout.addStretch()
        rtu_layout.addWidget(self.btn_rtu_connect)
        rtu_layout.addWidget(self.mb_rtu_status)
        self.mb_rtu_group.setLayout(rtu_layout)
        layout.addWidget(self.mb_rtu_group)

        # TCP 配置
        self.mb_tcp_group = QGroupBox("Modbus TCP")
        tcp_layout = QHBoxLayout()
        self.mb_tcp_host = QLineEdit("127.0.0.1")
        self.mb_tcp_port = QSpinBox()
        self.mb_tcp_port.setRange(1, 65535)
        self.mb_tcp_port.setValue(502)
        self.btn_tcp_connect = QPushButton("连接")
        self.btn_tcp_connect.clicked.connect(self._toggle_tcp)
        self.mb_tcp_status = QLabel("⚪ 未连接")
        self.mb_tcp_status.setStyleSheet("color:#aaa;")
        tcp_layout.addWidget(QLabel("主机:"))
        tcp_layout.addWidget(self.mb_tcp_host)
        tcp_layout.addWidget(QLabel("端口:"))
        tcp_layout.addWidget(self.mb_tcp_port)
        tcp_layout.addStretch()
        tcp_layout.addWidget(self.btn_tcp_connect)
        tcp_layout.addWidget(self.mb_tcp_status)
        self.mb_tcp_group.setLayout(tcp_layout)
        layout.addWidget(self.mb_tcp_group)

        # 操作区
        op_layout = QHBoxLayout()
        self.mb_func = QComboBox()
        self.mb_func.addItems(["读保持寄存器(03)", "写单个寄存器(06)", "写多个寄存器(16)"])
        self.mb_addr = QSpinBox()
        self.mb_addr.setRange(0, 65535)
        self.mb_count = QSpinBox()
        self.mb_count.setRange(1, 125)
        self.mb_count.setValue(10)
        self.mb_value = QSpinBox()
        self.mb_value.setRange(0, 65535)
        self.btn_mb_read = QPushButton("读取")
        self.btn_mb_read.clicked.connect(self._mb_read)
        self.btn_mb_write = QPushButton("写入")
        self.btn_mb_write.clicked.connect(self._mb_write)

        op_layout.addWidget(QLabel("功能:"))
        op_layout.addWidget(self.mb_func)
        op_layout.addWidget(QLabel("地址:"))
        op_layout.addWidget(self.mb_addr)
        op_layout.addWidget(QLabel("数量:"))
        op_layout.addWidget(self.mb_count)
        op_layout.addWidget(QLabel("值:"))
        op_layout.addWidget(self.mb_value)
        op_layout.addStretch()
        op_layout.addWidget(self.btn_mb_read)
        op_layout.addWidget(self.btn_mb_write)
        layout.addLayout(op_layout)

        # 显示
        self.mb_display = QTextEdit()
        self.mb_display.setReadOnly(True)
        self.mb_display.setFont(QFont("Consolas", 10))
        self.mb_display.setStyleSheet("background:#0a0a0a; color:#00ccff;")
        layout.addWidget(self.mb_display)

        self.mb_rtu_group.setVisible(False)
        self.mb_tcp_group.setVisible(True)

        return widget

    # ---------- CAN Tab ----------
    def _create_can_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # CAN 连接
        conn_layout = QHBoxLayout()
        self.can_interface = QComboBox()
        self.can_interface.addItems(["virtual", "socketcan"])
        self.can_channel = QLineEdit("vcan0")
        self.can_bitrate = QComboBox()
        self.can_bitrate.addItems(["125000", "250000", "500000", "1000000"])
        self.can_bitrate.setCurrentText("500000")
        self.btn_can_connect = QPushButton("连接")
        self.btn_can_connect.clicked.connect(self._toggle_can)
        self.can_status = QLabel("⚪ 未连接")
        self.can_status.setStyleSheet("color:#aaa;")

        conn_layout.addWidget(QLabel("接口:"))
        conn_layout.addWidget(self.can_interface)
        conn_layout.addWidget(QLabel("通道:"))
        conn_layout.addWidget(self.can_channel)
        conn_layout.addWidget(QLabel("波特率:"))
        conn_layout.addWidget(self.can_bitrate)
        conn_layout.addStretch()
        conn_layout.addWidget(self.btn_can_connect)
        conn_layout.addWidget(self.can_status)
        layout.addLayout(conn_layout)

        # CANopen
        co_group = QGroupBox("CANopen")
        co_layout = QHBoxLayout()
        self.co_node = QSpinBox()
        self.co_node.setRange(1, 127)
        self.co_node.setValue(1)
        self.co_index = QSpinBox()
        self.co_index.setRange(0x1000, 0xFFFF)
        self.co_index.setValue(0x1000)
        self.co_index.setDisplayIntegerBase(16)
        self.co_subindex = QSpinBox()
        self.co_subindex.setRange(0, 255)
        self.btn_co_read = QPushButton("SDO读取")
        self.btn_co_read.clicked.connect(self._co_read)
        self.btn_co_write = QPushButton("SDO写入")
        self.btn_co_write.clicked.connect(self._co_write)
        self.btn_co_nmt = QPushButton("NMT启动")
        self.btn_co_nmt.clicked.connect(self._co_nmt)

        co_layout.addWidget(QLabel("节点:"))
        co_layout.addWidget(self.co_node)
        co_layout.addWidget(QLabel("索引:"))
        co_layout.addWidget(self.co_index)
        co_layout.addWidget(QLabel("子索引:"))
        co_layout.addWidget(self.co_subindex)
        co_layout.addStretch()
        co_layout.addWidget(self.btn_co_read)
        co_layout.addWidget(self.btn_co_write)
        co_layout.addWidget(self.btn_co_nmt)
        co_group.setLayout(co_layout)
        layout.addWidget(co_group)

        # CAN 发送
        send_layout = QHBoxLayout()
        self.can_id = QSpinBox()
        self.can_id.setRange(0, 0x7FFFFFFF)
        self.can_id.setValue(0x123)
        self.can_data = QLineEdit("01 02 03 04")
        self.can_data.setFont(QFont("Consolas", 11))
        self.btn_can_send = QPushButton("发送")
        self.btn_can_send.clicked.connect(self._can_send)

        send_layout.addWidget(QLabel("ID:"))
        send_layout.addWidget(self.can_id)
        send_layout.addWidget(QLabel("数据:"))
        send_layout.addWidget(self.can_data)
        send_layout.addStretch()
        send_layout.addWidget(self.btn_can_send)
        layout.addLayout(send_layout)

        self.can_display = QTextEdit()
        self.can_display.setReadOnly(True)
        self.can_display.setFont(QFont("Consolas", 10))
        self.can_display.setStyleSheet("background:#0a0a0a; color:#ffaa00;")
        layout.addWidget(self.can_display)

        return widget

    # ---------- MQTT Tab ----------
    def _create_mqtt_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        conn_layout = QHBoxLayout()
        self.mqtt_broker = QLineEdit("localhost")
        self.mqtt_port = QSpinBox()
        self.mqtt_port.setRange(1, 65535)
        self.mqtt_port.setValue(1883)
        self.mqtt_client_id = QLineEdit("serial_monitor_v6")
        self.btn_mqtt_connect = QPushButton("连接")
        self.btn_mqtt_connect.clicked.connect(self._toggle_mqtt)
        self.mqtt_status = QLabel("⚪ 未连接")
        self.mqtt_status.setStyleSheet("color:#aaa;")

        conn_layout.addWidget(QLabel("Broker:"))
        conn_layout.addWidget(self.mqtt_broker)
        conn_layout.addWidget(QLabel("端口:"))
        conn_layout.addWidget(self.mqtt_port)
        conn_layout.addWidget(QLabel("Client ID:"))
        conn_layout.addWidget(self.mqtt_client_id)
        conn_layout.addStretch()
        conn_layout.addWidget(self.btn_mqtt_connect)
        conn_layout.addWidget(self.mqtt_status)
        layout.addLayout(conn_layout)

        pub_layout = QHBoxLayout()
        self.mqtt_topic = QLineEdit("serial/data")
        self.mqtt_payload = QLineEdit("")
        self.mqtt_payload.setPlaceholderText("要发布的数据")
        self.btn_mqtt_publish = QPushButton("发布")
        self.btn_mqtt_publish.clicked.connect(self._mqtt_publish)

        pub_layout.addWidget(QLabel("Topic:"))
        pub_layout.addWidget(self.mqtt_topic)
        pub_layout.addWidget(QLabel("Payload:"))
        pub_layout.addWidget(self.mqtt_payload)
        pub_layout.addStretch()
        pub_layout.addWidget(self.btn_mqtt_publish)
        layout.addLayout(pub_layout)

        self.mqtt_display = QTextEdit()
        self.mqtt_display.setReadOnly(True)
        self.mqtt_display.setFont(QFont("Consolas", 10))
        self.mqtt_display.setStyleSheet("background:#0a0a0a; color:#ff66ff;")
        layout.addWidget(self.mqtt_display)

        return widget

    # ---------- OPC UA Tab ----------
    def _create_opcua_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        conn_layout = QHBoxLayout()
        self.opcua_url = QLineEdit("opc.tcp://localhost:4840")
        self.btn_opcua_connect = QPushButton("连接")
        self.btn_opcua_connect.clicked.connect(self._toggle_opcua)
        self.opcua_status = QLabel("⚪ 未连接")
        self.opcua_status.setStyleSheet("color:#aaa;")

        conn_layout.addWidget(QLabel("URL:"))
        conn_layout.addWidget(self.opcua_url)
        conn_layout.addStretch()
        conn_layout.addWidget(self.btn_opcua_connect)
        conn_layout.addWidget(self.opcua_status)
        layout.addLayout(conn_layout)

        op_layout = QHBoxLayout()
        self.opcua_node = QLineEdit("ns=2;s=MyVariable")
        self.opcua_node.setMinimumWidth(200)
        self.opcua_value = QLineEdit("")
        self.btn_opcua_read = QPushButton("读取")
        self.btn_opcua_read.clicked.connect(self._opcua_read)
        self.btn_opcua_write = QPushButton("写入")
        self.btn_opcua_write.clicked.connect(self._opcua_write)

        op_layout.addWidget(QLabel("节点ID:"))
        op_layout.addWidget(self.opcua_node)
        op_layout.addWidget(QLabel("值:"))
        op_layout.addWidget(self.opcua_value)
        op_layout.addStretch()
        op_layout.addWidget(self.btn_opcua_read)
        op_layout.addWidget(self.btn_opcua_write)
        layout.addLayout(op_layout)

        self.opcua_display = QTextEdit()
        self.opcua_display.setReadOnly(True)
        self.opcua_display.setFont(QFont("Consolas", 10))
        self.opcua_display.setStyleSheet("background:#0a0a0a; color:#66ccff;")
        layout.addWidget(self.opcua_display)

        return widget

    # ---------- 曲线 Tab ----------
    def _create_chart_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        if CHART_AVAILABLE:
            self.chart_view = QChartView()
            self.chart_view.setRenderHint(QPainter.Antialiasing)
            self.chart_view.setStyleSheet("background:#0a0a0a; border:1px solid #333;")

            self.chart = QChart()
            self.chart.setTitle("实时数据曲线")
            self.chart.setTitleBrush(QBrush(QColor("#00ccff")))
            self.chart.setBackgroundBrush(QBrush(QColor("#0a0a0a")))
            self.chart.legend().setLabelColor(QColor("#aaa"))

            self.axis_x = QDateTimeAxis()
            self.axis_x.setFormat("hh:mm:ss")
            self.axis_x.setLabelsColor(QColor("#aaa"))
            self.chart.addAxis(self.axis_x, Qt.AlignBottom)

            self.axis_y = QValueAxis()
            self.axis_y.setLabelsColor(QColor("#aaa"))
            self.axis_y.setRange(0, 100)
            self.chart.addAxis(self.axis_y, Qt.AlignLeft)

            self.chart_view.setChart(self.chart)
            layout.addWidget(self.chart_view)

            # 控制
            ctrl = QHBoxLayout()
            self.chart_points = QSpinBox()
            self.chart_points.setRange(10, 1000)
            self.chart_points.setValue(100)
            self.chart_series_name = QLineEdit("value")
            self.chart_series_name.setPlaceholderText("系列名称")
            self.btn_chart_add = QPushButton("添加系列")
            self.btn_chart_add.clicked.connect(self._chart_add_series)
            self.btn_chart_clear = QPushButton("清空")
            self.btn_chart_clear.clicked.connect(self._chart_clear)

            ctrl.addWidget(QLabel("点数:"))
            ctrl.addWidget(self.chart_points)
            ctrl.addWidget(QLabel("名称:"))
            ctrl.addWidget(self.chart_series_name)
            ctrl.addStretch()
            ctrl.addWidget(self.btn_chart_add)
            ctrl.addWidget(self.btn_chart_clear)
            layout.addLayout(ctrl)
        else:
            layout.addWidget(QLabel("⚠️ PyQtChart 未安装，曲线功能不可用"))
            layout.addWidget(QLabel("请执行: pip install PyQtChart"))

        return widget

    # ---------- 自动化 Tab ----------
    def _create_auto_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # 脚本
        script_group = QGroupBox("脚本自动化")
        script_layout = QVBoxLayout()

        self.script_editor = QTextEdit()
        self.script_editor.setFont(QFont("Consolas", 10))
        self.script_editor.setStyleSheet("background:#0a0a0a; color:#00ff41;")
        self.script_editor.setPlaceholderText("""# Python 脚本示例
def on_start():
    log("脚本启动")
    return True

def on_timer():
    # 每秒执行
    log("定时任务执行")
    send_data("串口", b"AT\\r\\n")

def on_data(data):
    # 收到数据时触发
    log(f"收到: {data.hex()}")
    return data
""")
        script_layout.addWidget(self.script_editor)

        btn_layout = QHBoxLayout()
        self.btn_script_load = QPushButton("📂 加载脚本")
        self.btn_script_load.clicked.connect(self._load_script)
        self.btn_script_run = QPushButton("▶ 运行")
        self.btn_script_run.clicked.connect(self._run_script)
        self.btn_script_stop = QPushButton("⏹ 停止")
        self.btn_script_stop.clicked.connect(self._stop_script)
        self.script_status = QLabel("就绪")
        self.script_status.setStyleSheet("color:#aaa;")
        btn_layout.addWidget(self.btn_script_load)
        btn_layout.addWidget(self.btn_script_run)
        btn_layout.addWidget(self.btn_script_stop)
        btn_layout.addStretch()
        btn_layout.addWidget(self.script_status)
        script_layout.addLayout(btn_layout)

        self.script_output = QTextEdit()
        self.script_output.setReadOnly(True)
        self.script_output.setMaximumHeight(100)
        self.script_output.setStyleSheet("background:#0a0a0a; color:#ffaa00;")
        script_layout.addWidget(self.script_output)

        script_group.setLayout(script_layout)
        layout.addWidget(script_group)

        # 轮询
        polling_group = QGroupBox("多设备轮询")
        polling_layout = QVBoxLayout()

        poll_ctrl = QHBoxLayout()
        self.poll_name = QLineEdit("poll_1")
        self.poll_name.setPlaceholderText("任务名称")
        self.poll_interval = QSpinBox()
        self.poll_interval.setRange(100, 60000)
        self.poll_interval.setValue(1000)
        self.poll_interval.setSuffix(" ms")
        self.poll_action = QComboBox()
        self.poll_action.addItems(["modbus_read"])
        self.poll_slave = QSpinBox()
        self.poll_slave.setRange(1, 247)
        self.poll_slave.setValue(1)
        self.poll_addr = QSpinBox()
        self.poll_addr.setRange(0, 65535)
        self.poll_count = QSpinBox()
        self.poll_count.setRange(1, 125)
        self.poll_count.setValue(10)
        self.btn_poll_add = QPushButton("➕ 添加任务")
        self.btn_poll_add.clicked.connect(self._add_poll_task)

        poll_ctrl.addWidget(QLabel("名称:"))
        poll_ctrl.addWidget(self.poll_name)
        poll_ctrl.addWidget(QLabel("间隔:"))
        poll_ctrl.addWidget(self.poll_interval)
        poll_ctrl.addWidget(QLabel("动作:"))
        poll_ctrl.addWidget(self.poll_action)
        poll_ctrl.addWidget(QLabel("从站:"))
        poll_ctrl.addWidget(self.poll_slave)
        poll_ctrl.addWidget(QLabel("地址:"))
        poll_ctrl.addWidget(self.poll_addr)
        poll_ctrl.addWidget(QLabel("数量:"))
        poll_ctrl.addWidget(self.poll_count)
        poll_ctrl.addStretch()
        poll_ctrl.addWidget(self.btn_poll_add)
        polling_layout.addLayout(poll_ctrl)

        self.poll_table = QTableWidget()
        self.poll_table.setColumnCount(5)
        self.poll_table.setHorizontalHeaderLabels(["名称", "间隔(ms)", "动作", "参数", "操作"])
        self.poll_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.poll_table.setStyleSheet("background:#0a0a0a; color:#00ff41;")
        polling_layout.addWidget(self.poll_table)

        polling_group.setLayout(polling_layout)
        layout.addWidget(polling_group)

        return widget

    # ---------- 发送 Tab ----------
    def _create_send_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        target_layout = QHBoxLayout()
        self.send_target = QComboBox()
        self.send_target.addItems(["广播", "串口", "Modbus", "CAN", "MQTT"])
        target_layout.addWidget(QLabel("目标:"))
        target_layout.addWidget(self.send_target)
        target_layout.addStretch()
        layout.addLayout(target_layout)

        self.send_input = QTextEdit()
        self.send_input.setMaximumHeight(80)
        self.send_input.setFont(QFont("Consolas", 12))
        self.send_input.setStyleSheet("background:#0a0a0a; color:#fff; border:2px solid #444;")
        layout.addWidget(self.send_input)

        ctrl = QHBoxLayout()
        self.chk_hex = QCheckBox("HEX模式")
        self.chk_hex.setStyleSheet("color:#ccc;")
        self.chk_crlf = QCheckBox("自动\\r\\n")
        self.chk_crlf.setChecked(True)
        self.chk_crlf.setStyleSheet("color:#ccc;")

        self.btn_send = QPushButton("📤 发送")
        self.btn_send.setMinimumHeight(40)
        self.btn_send.setStyleSheet("font-weight:bold; background:#004d00;")
        self.btn_send.clicked.connect(self._send_data)

        ctrl.addWidget(self.chk_hex)
        ctrl.addWidget(self.chk_crlf)
        ctrl.addStretch()
        ctrl.addWidget(self.btn_send)
        layout.addLayout(ctrl)

        return widget

    # ---------- 设置 Tab ----------
    def _create_settings_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        log_group = QGroupBox("日志")
        log_layout = QHBoxLayout()
        self.chk_log = QCheckBox("启用日志")
        self.chk_log.setChecked(self.config.get("log_enabled", True))
        log_layout.addWidget(self.chk_log)
        log_layout.addStretch()
        log_group.setLayout(log_layout)
        layout.addWidget(log_group)

        trigger_group = QGroupBox("触发词报警")
        trigger_layout = QHBoxLayout()
        self.trigger_input = QLineEdit()
        self.trigger_input.setPlaceholderText("输入触发词，逗号分隔")
        self.btn_trigger_add = QPushButton("添加")
        self.btn_trigger_add.clicked.connect(self._add_trigger)
        trigger_layout.addWidget(self.trigger_input)
        trigger_layout.addWidget(self.btn_trigger_add)
        trigger_group.setLayout(trigger_layout)
        layout.addWidget(trigger_group)

        layout.addStretch()
        return widget

    # ---------- 菜单 ----------
    def _setup_menu(self):
        menubar = self.menuBar()
        menubar.setStyleSheet("QMenuBar{background:#1a1a1a; color:#ccc; border-bottom:1px solid #333;}")
        file_menu = menubar.addMenu("文件")
        file_menu.addAction("导出日志", self._export_log)
        file_menu.addAction("导出配置", self._export_config)
        file_menu.addSeparator()
        file_menu.addAction("退出", self.close)

        tools_menu = menubar.addMenu("工具")
        tools_menu.addAction("CRC16计算器", self._show_crc)

        help_menu = menubar.addMenu("帮助")
        help_menu.addAction("关于 v6.0", self._show_about)

    # ---------- 工具栏 ----------
    def _setup_toolbar(self):
        toolbar = self.addToolBar("工具栏")
        toolbar.setMovable(False)
        toolbar.setStyleSheet("QToolBar{background:#1a1a1a; border:none; border-bottom:1px solid #333; padding:2px 6px;}")

        toolbar.addWidget(QLabel("端口:"))
        self.tb_port = QComboBox()
        self.tb_port.setMinimumWidth(120)
        toolbar.addWidget(self.tb_port)

        toolbar.addWidget(QLabel("波特率:"))
        self.tb_baud = QComboBox()
        self.tb_baud.addItems(["9600", "19200", "38400", "57600", "115200", "230400"])
        self.tb_baud.setCurrentText("115200")
        self.tb_baud.setMinimumWidth(100)
        toolbar.addWidget(self.tb_baud)

        toolbar.addSeparator()

        self.tb_hex = QCheckBox("HEX")
        self.tb_hex.setChecked(self.config.get("hex_mode", False))
        toolbar.addWidget(self.tb_hex)

        btn_clear = QPushButton("清屏")
        btn_clear.clicked.connect(lambda: self.display_area.clear())
        toolbar.addWidget(btn_clear)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        toolbar.addWidget(spacer)

        self.tb_status = QLabel("⚪ 未连接")
        self.tb_status.setStyleSheet("color:#888; padding:0 10px;")
        toolbar.addWidget(self.tb_status)

        self.tb_counter = QLabel("RX:0 TX:0")
        self.tb_counter.setStyleSheet("color:#666; padding:0 10px;")
        toolbar.addWidget(self.tb_counter)

    # ---------- 状态栏 ----------
    def _setup_statusbar(self):
        self.statusBar().setStyleSheet("background:#1a1a1a; color:#888; border-top:1px solid #333;")
        self.status_label = QLabel("就绪 v6.0")
        self.status_label.setStyleSheet("color:#aaa; padding:2px 8px;")
        self.statusBar().addWidget(self.status_label, 1)

        self.status_rx = QLabel("RX:0")
        self.status_rx.setStyleSheet("color:#00ff41; padding:2px 8px;")
        self.statusBar().addPermanentWidget(self.status_rx)

        self.status_tx = QLabel("TX:0")
        self.status_tx.setStyleSheet("color:#ffaa00; padding:2px 8px;")
        self.statusBar().addPermanentWidget(self.status_tx)

    # ---------- 信号连接 ----------
    def _setup_connections(self):
        self.data_bus.data_received.connect(self._on_data)
        self.data_bus.device_status.connect(self._on_device_status)
        self.script_engine.script_executed.connect(self._on_script_output)
        self.script_engine.error_occurred.connect(lambda e: self._append_display(f"[脚本错误] {e}", "#ff4444"))

    # ---------- 核心方法 ----------
    def _refresh_ports(self):
        self.port_combo.clear()
        self.tb_port.clear()
        self.mb_rtu_port.clear()
        for p in serial.tools.list_ports.comports():
            self.port_combo.addItem(p.device)
            self.tb_port.addItem(p.device)
            self.mb_rtu_port.addItem(p.device)

    def _add_serial(self):
        port = self.port_combo.currentText()
        baud = int(self.baud_combo.currentText())
        if not port:
            return
        port_id = self.serial_manager.add_port(port, baud)
        self._update_serial_table()
        self._log_system(f"添加串口: {port} @ {baud}")

    def _update_serial_table(self):
        self.serial_table.setRowCount(0)
        for i, (pid, port, baud, running) in enumerate(self.serial_manager.get_ports()):
            self.serial_table.insertRow(i)
            self.serial_table.setItem(i, 0, QTableWidgetItem(pid))
            self.serial_table.setItem(i, 1, QTableWidgetItem(port))
            self.serial_table.setItem(i, 2, QTableWidgetItem(str(baud)))
            status = "🟢 在线" if running else "🔴 离线"
            self.serial_table.setItem(i, 3, QTableWidgetItem(status))

    def _mb_mode_changed(self, idx):
        self.mb_rtu_group.setVisible(idx == 0)
        self.mb_tcp_group.setVisible(idx == 1)

    # ---------- Modbus RTU ----------
    def _toggle_rtu(self):
        if self.modbus_rtu and self.modbus_rtu.isRunning():
            self.modbus_rtu.stop()
            self.modbus_rtu = None
            self.btn_rtu_connect.setText("连接")
            self.mb_rtu_status.setText("⚪ 未连接")
            self.mb_rtu_status.setStyleSheet("color:#aaa;")
            self._log_system("Modbus RTU 已断开")
            return

        port = self.mb_rtu_port.currentText()
        baud = int(self.mb_rtu_baud.currentText())
        slave = self.mb_rtu_slave.value()
        if not port:
            QMessageBox.warning(self, "警告", "请选择串口")
            return

        self.modbus_rtu = ModbusRTUManager(port, baud, slave)
        self.modbus_rtu.start()
        self.btn_rtu_connect.setText("断开")
        self.mb_rtu_status.setText("🔄 连接中...")
        self.mb_rtu_status.setStyleSheet("color:#ffaa00;")
        self._log_system(f"Modbus RTU 连接中: {port} @ {baud}")

    # ---------- Modbus TCP ----------
    def _toggle_tcp(self):
        if self.modbus_tcp and self.modbus_tcp.isRunning():
            self.modbus_tcp.stop()
            self.modbus_tcp = None
            self.btn_tcp_connect.setText("连接")
            self.mb_tcp_status.setText("⚪ 未连接")
            self.mb_tcp_status.setStyleSheet("color:#aaa;")
            self._log_system("Modbus TCP 已断开")
            return

        host = self.mb_tcp_host.text()
        port = self.mb_tcp_port.value()
        self.modbus_tcp = ModbusTCPManager(host, port)
        self.modbus_tcp.start()
        self.btn_tcp_connect.setText("断开")
        self.mb_tcp_status.setText("🔄 连接中...")
        self.mb_tcp_status.setStyleSheet("color:#ffaa00;")
        self._log_system(f"Modbus TCP 连接中: {host}:{port}")

    # ---------- Modbus 操作 ----------
    def _mb_read(self):
        func = self.mb_func.currentIndex()
        addr = self.mb_addr.value()
        count = self.mb_count.value()
        slave = self.mb_rtu_slave.value() if self.mb_mode.currentIndex() == 0 else 1

        if self.mb_mode.currentIndex() == 0:  # RTU
            if self.modbus_rtu and self.modbus_rtu.isRunning():
                self.modbus_rtu.read_holding(addr, count)
                self._log_system(f"Modbus RTU 读取: 地址{addr} 数量{count}")
            else:
                QMessageBox.warning(self, "警告", "请先连接 Modbus RTU")
        else:  # TCP
            if self.modbus_tcp and self.modbus_tcp.isRunning():
                result = self.modbus_tcp.read_registers(addr, count, slave)
                if result and not result.isError():
                    self.mb_display.append(f"[读取] 地址:{addr} 数量:{count} 值:{result.registers}")
                    self._log_system(f"Modbus TCP 读取: {result.registers}")
                else:
                    self.mb_display.append(f"[读取] 失败: {result}")
            else:
                QMessageBox.warning(self, "警告", "请先连接 Modbus TCP")

    def _mb_write(self):
        func = self.mb_func.currentIndex()
        addr = self.mb_addr.value()
        value = self.mb_value.value()

        if self.mb_mode.currentIndex() == 0:  # RTU
            if self.modbus_rtu and self.modbus_rtu.isRunning():
                if func == 0:  # 写单个
                    self.modbus_rtu.write_single(addr, value)
                    self._log_system(f"Modbus RTU 写入: 地址{addr} 值{value}")
                else:
                    QMessageBox.information(self, "提示", "RTU 写多个请使用单个写入")
            else:
                QMessageBox.warning(self, "警告", "请先连接 Modbus RTU")
        else:  # TCP
            if self.modbus_tcp and self.modbus_tcp.isRunning():
                result = self.modbus_tcp.write_register(addr, value)
                if result and not result.isError():
                    self.mb_display.append(f"[写入] 地址:{addr} 值:{value}")
                    self._log_system(f"Modbus TCP 写入: 地址{addr} 值{value}")
                else:
                    self.mb_display.append(f"[写入] 失败")
            else:
                QMessageBox.warning(self, "警告", "请先连接 Modbus TCP")

    # ---------- CAN ----------
    def _toggle_can(self):
        if self.can_manager and self.can_manager.isRunning():
            self.can_manager.stop()
            self.can_manager = None
            self.canopen_manager = None
            self.btn_can_connect.setText("连接")
            self.can_status.setText("⚪ 未连接")
            self.can_status.setStyleSheet("color:#aaa;")
            self._log_system("CAN 已断开")
            return

        interface = self.can_interface.currentText()
        channel = self.can_channel.text()
        bitrate = int(self.can_bitrate.currentText())

        self.can_manager = CANManager(interface, channel, bitrate)
        self.can_manager.start()
        self.canopen_manager = CANopenManager(self.can_manager)
        self.canopen_manager.set_node_id(self.co_node.value())

        self.btn_can_connect.setText("断开")
        self.can_status.setText("🔄 连接中...")
        self.can_status.setStyleSheet("color:#ffaa00;")
        self._log_system("CAN 连接中...")

    def _can_send(self):
        if not self.can_manager or not self.can_manager.isRunning():
            QMessageBox.warning(self, "警告", "请先连接CAN")
            return

        can_id = self.can_id.value()
        try:
            data = bytes.fromhex(self.can_data.text().replace(" ", ""))
            self.can_manager.send(can_id, data)
            self.can_display.append(f"[发送] ID:{can_id:03X} Data:{data.hex(' ').upper()}")
            self._log_system(f"CAN发送: ID:{can_id:03X}")
        except Exception as e:
            QMessageBox.critical(self, "错误", str(e))

    # ---------- CANopen ----------
    def _co_read(self):
        if not self.canopen_manager:
            QMessageBox.warning(self, "警告", "请先连接CAN")
            return
        index = self.co_index.value()
        subindex = self.co_subindex.value()
        self.canopen_manager.sdo_read(index, subindex)
        self._log_system(f"CANopen SDO读取: 节点{self.co_node.value()} 索引{index:04X}.{subindex}")

    def _co_write(self):
        if not self.canopen_manager:
            QMessageBox.warning(self, "警告", "请先连接CAN")
            return
        index = self.co_index.value()
        subindex = self.co_subindex.value()
        self.canopen_manager.sdo_write(index, subindex, 0x1234)
        self._log_system(f"CANopen SDO写入: 节点{self.co_node.value()}")

    def _co_nmt(self):
        if not self.canopen_manager:
            QMessageBox.warning(self, "警告", "请先连接CAN")
            return
        self.canopen_manager.nmt_start()
        self._log_system(f"CANopen NMT启动: 节点{self.co_node.value()}")

    # ---------- MQTT ----------
    def _toggle_mqtt(self):
        if self.mqtt_manager and self.mqtt_manager.isRunning():
            self.mqtt_manager.stop()
            self.mqtt_manager = None
            self.btn_mqtt_connect.setText("连接")
            self.mqtt_status.setText("⚪ 未连接")
            self.mqtt_status.setStyleSheet("color:#aaa;")
            self._log_system("MQTT 已断开")
            return

        broker = self.mqtt_broker.text()
        port = self.mqtt_port.value()
        client_id = self.mqtt_client_id.text()

        self.mqtt_manager = MQTTManager(broker, port, client_id)
        self.mqtt_manager.start()
        self.btn_mqtt_connect.setText("断开")
        self.mqtt_status.setText("🔄 连接中...")
        self.mqtt_status.setStyleSheet("color:#ffaa00;")
        self._log_system("MQTT 连接中...")

    def _mqtt_publish(self):
        if not self.mqtt_manager or not self.mqtt_manager.isRunning():
            QMessageBox.warning(self, "警告", "请先连接MQTT")
            return
        topic = self.mqtt_topic.text()
        payload = self.mqtt_payload.text()
        if payload:
            self.mqtt_manager.publish(topic, payload)
            self.mqtt_display.append(f"[发布] {topic}: {payload}")
            self._log_system(f"MQTT发布: {topic}")

    # ---------- OPC UA ----------
    def _toggle_opcua(self):
        if self.opcua_manager and self.opcua_manager.isRunning():
            self.opcua_manager.stop()
            self.opcua_manager = None
            self.btn_opcua_connect.setText("连接")
            self.opcua_status.setText("⚪ 未连接")
            self.opcua_status.setStyleSheet("color:#aaa;")
            self._log_system("OPC UA 已断开")
            return

        url = self.opcua_url.text()
        self.opcua_manager = OPCUAManager(url)
        self.opcua_manager.start()
        self.btn_opcua_connect.setText("断开")
        self.opcua_status.setText("🔄 连接中...")
        self.opcua_status.setStyleSheet("color:#ffaa00;")
        self._log_system("OPC UA 连接中...")

    def _opcua_read(self):
        if not self.opcua_manager or not self.opcua_manager.isRunning():
            QMessageBox.warning(self, "警告", "请先连接OPC UA")
            return
        node_id = self.opcua_node.text()
        result = self.opcua_manager.read_node(node_id)
        if result is not None:
            self.opcua_display.append(f"[读取] {node_id} = {result}")
            self._log_system(f"OPC UA读取: {node_id} = {result}")
        else:
            self.opcua_display.append(f"[读取] {node_id} 失败")

    def _opcua_write(self):
        if not self.opcua_manager or not self.opcua_manager.isRunning():
            QMessageBox.warning(self, "警告", "请先连接OPC UA")
            return
        node_id = self.opcua_node.text()
        value = self.opcua_value.text()
        try:
            # 尝试转换为数字
            if '.' in value:
                val = float(value)
            else:
                val = int(value) if value.isdigit() else value
        except:
            val = value
        result = self.opcua_manager.write_node(node_id, val)
        if result is True:
            self.opcua_display.append(f"[写入] {node_id} = {val}")
            self._log_system(f"OPC UA写入: {node_id} = {val}")
        else:
            self.opcua_display.append(f"[写入] 失败")

    # ---------- 图表 ----------
    def _chart_add_series(self):
        if not CHART_AVAILABLE:
            return
        name = self.chart_series_name.text()
        if not name:
            return
        series = QLineSeries()
        series.setName(name)
        self.chart.addSeries(series)
        series.attachAxis(self.axis_x)
        series.attachAxis(self.axis_y)
        self.chart_series[name] = series
        self._log_system(f"添加图表系列: {name}")

    def _chart_clear(self):
        if not CHART_AVAILABLE:
            return
        for s in self.chart.series():
            self.chart.removeSeries(s)
        self.chart_series.clear()
        self._log_system("图表已清空")

    # ---------- 脚本 ----------
    def _load_script(self):
        path, _ = QFileDialog.getOpenFileName(self, "加载脚本", "", "Python (*.py)")
        if path:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    self.script_editor.setText(f.read())
                self._log_system(f"脚本加载: {path}")
            except Exception as e:
                QMessageBox.critical(self, "错误", str(e))

    def _run_script(self):
        code = self.script_editor.toPlainText()
        if not code:
            return
        success = self.script_engine.load_script("main", code)
        if success:
            self.script_status.setText("✅ 运行中")
            self.script_status.setStyleSheet("color:#00ff41;")
            self.script_engine.start_timer(1000)
            self.script_engine.execute_function("main", "on_start")
            self._log_system("脚本已运行")
        else:
            self.script_status.setText("❌ 加载失败")
            self.script_status.setStyleSheet("color:#ff4444;")

    def _stop_script(self):
        self.script_engine.stop_timer()
        self.script_status.setText("⏹ 已停止")
        self.script_status.setStyleSheet("color:#ff4444;")
        self._log_system("脚本已停止")

    def _on_script_output(self, func, result):
        self.script_output.append(f"[{func}] {result}")
        self._append_display(f"[脚本] {func}: {result}", "#ffaa00")

    # ---------- 轮询 ----------
    def _add_poll_task(self):
        name = self.poll_name.text()
        interval = self.poll_interval.value()
        action = self.poll_action.currentText()
        params = {
            "slave": self.poll_slave.value(),
            "address": self.poll_addr.value(),
            "count": self.poll_count.value()
        }

        task = PollingTask(name, "polling", interval, action, params)
        self.polling_engine.add_task(task)

        row = self.poll_table.rowCount()
        self.poll_table.insertRow(row)
        self.poll_table.setItem(row, 0, QTableWidgetItem(name))
        self.poll_table.setItem(row, 1, QTableWidgetItem(str(interval)))
        self.poll_table.setItem(row, 2, QTableWidgetItem(action))
        self.poll_table.setItem(row, 3, QTableWidgetItem(str(params)))

        btn = QPushButton("删除")
        btn.clicked.connect(lambda: self._remove_poll_task(name))
        self.poll_table.setCellWidget(row, 4, btn)

        self._log_system(f"添加轮询任务: {name}")

    def _remove_poll_task(self, name):
        self.polling_engine.remove_task(name)
        for row in range(self.poll_table.rowCount()):
            if self.poll_table.item(row, 0).text() == name:
                self.poll_table.removeRow(row)
                break
        self._log_system(f"删除轮询任务: {name}")

    # ---------- 发送 ----------
    def _send_data(self):
        text = self.send_input.toPlainText().strip()
        if not text:
            return

        target = self.send_target.currentText()

        try:
            if self.chk_hex.isChecked():
                data = bytes.fromhex(text.replace(" ", "").replace("\n", ""))
            else:
                data = text.encode('utf-8')
                if self.chk_crlf.isChecked():
                    data += b"\r\n"

            if target == "广播":
                self.serial_manager.broadcast(data)
                if self.can_manager:
                    self.can_manager.send(0x100, data)
                self._log_system(f"广播发送: {data.hex(' ').upper()[:50]}...")
            elif target == "串口":
                self.serial_manager.broadcast(data)
                self._log_system(f"串口发送: {data.hex(' ').upper()[:50]}...")
            elif target == "Modbus":
                if self.modbus_rtu and self.modbus_rtu.isRunning():
                    self.modbus_rtu.send_queue.append(data)
                    self._log_system(f"Modbus RTU发送: {data.hex(' ').upper()[:50]}...")
                else:
                    QMessageBox.warning(self, "警告", "Modbus RTU未连接")
            elif target == "CAN":
                if self.can_manager and self.can_manager.isRunning():
                    self.can_manager.send(0x100, data)
                    self._log_system(f"CAN发送: {data.hex(' ').upper()[:50]}...")
                else:
                    QMessageBox.warning(self, "警告", "CAN未连接")
            elif target == "MQTT":
                if self.mqtt_manager and self.mqtt_manager.isRunning():
                    self.mqtt_manager.publish("serial/data", text)
                    self._log_system(f"MQTT发送: {text[:50]}...")
                else:
                    QMessageBox.warning(self, "警告", "MQTT未连接")

            self.tx_count += len(data)
            self._update_status()
            self.send_input.clear()
        except ValueError:
            QMessageBox.warning(self, "格式错误", "HEX模式下请输入合法十六进制")
        except Exception as e:
            QMessageBox.critical(self, "发送失败", str(e))

    # ---------- 触发词 ----------
    def _add_trigger(self):
        text = self.trigger_input.text().strip()
        if not text:
            return
        keywords = [kw.strip() for kw in text.split(",") if kw.strip()]
        self.trigger_input.clear()
        self._log_system(f"已添加触发词: {', '.join(keywords)}")

    # ---------- 数据接收 ----------
    def _on_data(self, packet: DataPacket):
        if packet.data:
            self.rx_count += len(packet.data)
            if self.chk_hex.isChecked():
                display = packet.data.hex(' ').upper()
            else:
                display = packet.data.decode('utf-8', errors='replace')
            self._append_display(f"[{packet.source}] {display[:200]}")
            self._update_status()

    def _on_device_status(self, device_id: str, connected: bool, message: str):
        self._append_display(f"[状态] {device_id}: {message}", "#ffaa00" if connected else "#ff4444")

        # 更新各模块状态显示
        if "modbus_rtu" in device_id:
            self.mb_rtu_status.setText("🟢 已连接" if connected else "🔴 已断开")
            self.mb_rtu_status.setStyleSheet("color:#00ff41;" if connected else "color:#ff4444;")
        if "modbus_tcp" in device_id:
            self.mb_tcp_status.setText("🟢 已连接" if connected else "🔴 已断开")
            self.mb_tcp_status.setStyleSheet("color:#00ff41;" if connected else "color:#ff4444;")
        if "can" in device_id:
            self.can_status.setText("🟢 已连接" if connected else "🔴 已断开")
            self.can_status.setStyleSheet("color:#00ff41;" if connected else "color:#ff4444;")
        if "mqtt" in device_id:
            self.mqtt_status.setText("🟢 已连接" if connected else "🔴 已断开")
            self.mqtt_status.setStyleSheet("color:#00ff41;" if connected else "color:#ff4444;")
        if "opcua" in device_id:
            self.opcua_status.setText("🟢 已连接" if connected else "🔴 已断开")
            self.opcua_status.setStyleSheet("color:#00ff41;" if connected else "color:#ff4444;")

    # ---------- 辅助 ----------
    def _append_display(self, text, color="#00ff41"):
        ts = datetime.now().strftime("%H:%M:%S")
        self.display_area.append(f'<span style="color:#666;">[{ts}]</span> <span style="color:{color};">{text}</span>')
        self.display_area.verticalScrollBar().setValue(self.display_area.verticalScrollBar().maximum())

    def _log_system(self, text):
        self._append_display(f"[系统] {text}", "#ffff00")

    def _update_status(self):
        self.status_rx.setText(f"RX:{self.rx_count}")
        self.status_tx.setText(f"TX:{self.tx_count}")
        self.tb_counter.setText(f"RX:{self.rx_count} TX:{self.tx_count}")
        port_count = self.serial_manager.get_count()
        self.status_label.setText(f"就绪 | 串口:{port_count} | RX:{self.rx_count} TX:{self.tx_count}")

    def _load_config(self):
        self.chk_hex.setChecked(self.config.get("hex_mode", False))
        self.tb_hex.setChecked(self.config.get("hex_mode", False))
        self.chk_log.setChecked(self.config.get("log_enabled", True))

    # ---------- 导出 ----------
    def _export_log(self):
        path, _ = QFileDialog.getSaveFileName(self, "导出日志", "", "*.txt")
        if path:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(self.display_area.toPlainText())
                QMessageBox.information(self, "成功", f"已导出到 {path}")
            except Exception as e:
                QMessageBox.critical(self, "失败", str(e))

    def _export_config(self):
        self.config.save()
        QMessageBox.information(self, "成功", "配置已保存")

    def _show_crc(self):
        QMessageBox.information(self, "CRC16计算器", "请使用 5.2 版本的 CRC 功能")

    def _show_about(self):
        QMessageBox.about(self, "关于",
            "🏭 工业串口监控系统 v6.0\n\n"
            "功能模块:\n"
            "• 串口 (多通道/自动重连)\n"
            "• Modbus RTU (03/06/16)\n"
            "• Modbus TCP (03/06)\n"
            "• CAN (收发)\n"
            "• CANopen (NMT/SDO)\n"
            "• MQTT (发布/订阅)\n"
            "• OPC UA (客户端读写)\n"
            "• 实时数据曲线\n"
            "• 脚本自动化 (Python)\n"
            "• 多设备轮询\n\n"
            "工业级完整调试工具链"
        )

    # ---------- 关闭 ----------
    def closeEvent(self, event):
        self.polling_engine.stop()
        self.serial_manager.remove_all()
        if self.modbus_tcp:
            self.modbus_tcp.stop()
        if self.modbus_rtu:
            self.modbus_rtu.stop()
        if self.can_manager:
            self.can_manager.stop()
        if self.mqtt_manager:
            self.mqtt_manager.stop()
        if self.opcua_manager:
            self.opcua_manager.stop()
        self.config.save()
        event.accept()


# ============================================
# 启动
# ============================================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(26, 26, 26))
    palette.setColor(QPalette.WindowText, QColor(204, 204, 204))
    palette.setColor(QPalette.Base, QColor(10, 10, 10))
    palette.setColor(QPalette.AlternateBase, QColor(42, 42, 42))
    palette.setColor(QPalette.Text, QColor(204, 204, 204))
    palette.setColor(QPalette.Button, QColor(42, 42, 42))
    palette.setColor(QPalette.ButtonText, QColor(204, 204, 204))
    palette.setColor(QPalette.Highlight, QColor(0, 85, 170))
    palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
    app.setPalette(palette)

    window = IndustrialSerialV6()
    window.show()
    sys.exit(app.exec_())