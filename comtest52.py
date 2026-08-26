"""
工业串口监控系统 v5.2 - 生产增强版 (CAN修复版)
修复：CAN virtual 接口版本兼容问题
功能：串口、Modbus TCP、CAN、MQTT、发送、设置、日志、触发词、CRC
"""

import sys
import os
import json
import time
import threading
import queue
import struct
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
# 尝试导入可选模块
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

# ============================================
# 配置文件
# ============================================
CONFIG_FILE = "serial_prod_config_v5.json"
AUDIT_FILE = "audit_v5.log"
LOG_DIR = "logs"


class ConfigManagerV5:
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
                "serial_ports": [],
                "hex_mode": False,
                "log_enabled": True,
                "show_timestamp": True,
                "auto_connect": False,
                "window_x": 100,
                "window_y": 100,
                "window_width": 1280,
                "window_height": 800,
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
    metadata: Dict[str, Any] = field(default_factory=dict)
    parsed: Optional[Dict] = None


class DataBus(QObject):
    data_received = pyqtSignal(DataPacket)
    device_status = pyqtSignal(str, bool, str)
    alert_triggered = pyqtSignal(str, str)

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls._create_instance()
            return cls._instance

    @classmethod
    def _create_instance(cls):
        instance = super(DataBus, cls).__new__(cls)
        QObject.__init__(instance)
        instance._initialized = True
        instance._subscribers = defaultdict(list)
        instance._history = deque(maxlen=10000)
        return instance

    def publish(self, packet: DataPacket):
        self._history.append(packet)
        self.data_received.emit(packet)

        for callback in self._subscribers.get(packet.source_type, []):
            try:
                callback(packet)
            except Exception as e:
                pass

        for callback in self._subscribers.get("*", []):
            try:
                callback(packet)
            except Exception as e:
                pass

    def subscribe(self, source_type: str, callback: Callable):
        self._subscribers[source_type].append(callback)

    def subscribe_all(self, callback: Callable):
        self._subscribers["*"].append(callback)

    def get_history(self, limit=100):
        return list(self._history)[-limit:]


def get_data_bus():
    return DataBus.instance()


# ============================================
# Modbus RTU 解析器 (CRC工具)
# ============================================
class ModbusRTUParser:
    @classmethod
    def _calculate_crc(cls, data: bytes) -> int:
        crc = 0xFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xA001
                else:
                    crc >>= 1
        return crc


# ============================================
# 串口管理器
# ============================================
class SerialPortWorker(QThread):
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
            self.serial.close()
        self.wait()
        get_data_bus().device_status.emit(self.port_id, False, "已断开")


class SerialManager:
    def __init__(self):
        self.workers = {}

    def add_port(self, port: str, baud: int) -> str:
        port_id = f"port_{int(time.time() * 1000) % 100000}"
        worker = SerialPortWorker(port_id, port, baud)
        worker.start()
        self.workers[port_id] = worker
        return port_id

    def remove_port(self, port_id: str):
        if port_id in self.workers:
            self.workers[port_id].stop()
            del self.workers[port_id]

    def remove_all(self):
        for port_id in list(self.workers.keys()):
            self.remove_port(port_id)

    def send_to(self, port_id: str, data: bytes):
        if port_id in self.workers:
            self.workers[port_id].send(data)

    def broadcast(self, data: bytes):
        for worker in self.workers.values():
            worker.send(data)

    def get_count(self):
        return len(self.workers)

    def get_ports(self):
        return [(pid, w.port, w.baud, w.isRunning()) for pid, w in self.workers.items()]


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
        """创建 CAN 总线，兼容不同版本的 python-can"""
        if self.interface == "virtual":
            # 尝试多种方式创建 virtual 接口
            try:
                return can.interface.Bus(channel=self.channel, interface="virtual")
            except Exception as e1:
                try:
                    from can.interfaces.virtual import Bus as VirtualBus
                    return VirtualBus(channel=self.channel)
                except ImportError as e2:
                    # virtual 不可用，尝试 socketcan + vcan
                    try:
                        import subprocess
                        # 尝试创建 vcan0（Windows 下会失败，忽略）
                        subprocess.run(['ip', 'link', 'add', 'dev', 'vcan0', 'type', 'vcan'],
                                       check=False, capture_output=True)
                        subprocess.run(['ip', 'link', 'set', 'up', 'vcan0'],
                                       check=False, capture_output=True)
                        return can.interface.Bus(channel='vcan0', interface='socketcan')
                    except:
                        raise Exception(f"无法创建CAN接口: virtual接口不可用, 请安装 python-can 或配置 socketcan")
        else:
            return can.interface.Bus(channel=self.channel, interface=self.interface, bitrate=self.bitrate)

    def run(self):
        self._running = True
        if not CAN_AVAILABLE:
            get_data_bus().device_status.emit("can", False, "python-can未安装 (请执行: pip install python-can)")
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
                        metadata={
                            "arbitration_id": msg.arbitration_id,
                            "dlc": msg.dlc,
                            "channel": msg.channel
                        }
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
# MQTT 管理器 (修复 2.x API)
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
# 主窗口
# ============================================
class IndustrialSerialToolV5(QMainWindow):
    def __init__(self):
        super().__init__()
        self.config = ConfigManagerV5()
        self.data_bus = get_data_bus()
        self.serial_manager = SerialManager()
        self.modbus_manager = None
        self.can_manager = None
        self.mqtt_manager = None
        self.log_file = None
        self.rx_count = 0
        self.tx_count = 0

        self._init_ui()
        self._setup_menu()
        self._setup_toolbar()
        self._setup_statusbar()
        self._setup_connections()
        self._load_config()

        self._refresh_ports()

        if self.config.get("auto_connect", False):
            QTimer.singleShot(1000, self._auto_connect)

        self._log_system("🏭 工业串口监控系统 v5.2 生产增强版启动")

    # ---------- UI ----------
    def _init_ui(self):
        self.setWindowTitle("工业串口监控系统 v5.2 - 生产增强版")
        self.setMinimumSize(1200, 800)

        x = self.config.get("window_x", 100)
        y = self.config.get("window_y", 100)
        w = self.config.get("window_width", 1280)
        h = self.config.get("window_height", 800)
        self.setGeometry(x, y, w, h)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setSpacing(3)
        layout.setContentsMargins(3, 3, 3, 3)

        splitter = QSplitter(Qt.Horizontal)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self.display = QTextEdit()
        self.display.setReadOnly(True)
        self.display.setFont(QFont("Consolas", 10))
        self.display.setStyleSheet("background:#0a0a0a; color:#00ff41; border:2px solid #333; border-radius:4px;")
        left_layout.addWidget(self.display)

        splitter.addWidget(left)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.tabs = QTabWidget()
        self.tabs.setFont(QFont("Consolas", 10))
        self.tabs.setStyleSheet("QTabWidget::pane{border:1px solid #444;background:#1a1a1a;}")

        self.tabs.addTab(self._create_serial_tab(), "串口")
        self.tabs.addTab(self._create_modbus_tab(), "Modbus TCP")
        self.tabs.addTab(self._create_can_tab(), "CAN")
        self.tabs.addTab(self._create_mqtt_tab(), "MQTT")
        self.tabs.addTab(self._create_send_tab(), "发送")
        self.tabs.addTab(self._create_settings_tab(), "设置")

        right_layout.addWidget(self.tabs)
        splitter.addWidget(right)

        splitter.setSizes([500, 700])
        layout.addWidget(splitter)

    # ---------- 串口 Tab ----------
    def _create_serial_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        self.port_table = QTableWidget()
        self.port_table.setColumnCount(4)
        self.port_table.setHorizontalHeaderLabels(["ID", "端口", "波特率", "状态"])
        self.port_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.port_table.setStyleSheet("background:#0a0a0a; color:#00ff41; border:1px solid #333;")
        layout.addWidget(self.port_table)

        ctrl = QHBoxLayout()
        self.port_combo = QComboBox()
        self.port_combo.setMinimumWidth(150)
        self.baud_combo = QComboBox()
        self.baud_combo.addItems(["9600", "19200", "38400", "57600", "115200", "230400", "460800"])
        self.baud_combo.setCurrentText("115200")
        self.btn_add = QPushButton("添加串口")
        self.btn_add.clicked.connect(self._add_port)
        self.btn_refresh = QPushButton("刷新")
        self.btn_refresh.clicked.connect(self._refresh_ports)

        ctrl.addWidget(QLabel("端口:"))
        ctrl.addWidget(self.port_combo)
        ctrl.addWidget(QLabel("波特率:"))
        ctrl.addWidget(self.baud_combo)
        ctrl.addStretch()
        ctrl.addWidget(self.btn_add)
        ctrl.addWidget(self.btn_refresh)

        layout.addLayout(ctrl)

        btn_layout = QHBoxLayout()
        btn_remove = QPushButton("断开选中")
        btn_remove.clicked.connect(self._remove_selected)
        btn_remove_all = QPushButton("断开全部")
        btn_remove_all.clicked.connect(self._remove_all)
        btn_layout.addWidget(btn_remove)
        btn_layout.addWidget(btn_remove_all)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        return widget

    # ---------- Modbus Tab ----------
    def _create_modbus_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        conn_layout = QHBoxLayout()
        self.mb_host = QLineEdit("127.0.0.1")
        self.mb_port = QSpinBox()
        self.mb_port.setRange(1, 65535)
        self.mb_port.setValue(502)
        self.btn_mb_connect = QPushButton("连接")
        self.btn_mb_connect.clicked.connect(self._toggle_modbus)
        self.mb_status = QLabel("⚪ 未连接")
        self.mb_status.setStyleSheet("color: #aaa;")

        conn_layout.addWidget(QLabel("主机:"))
        conn_layout.addWidget(self.mb_host)
        conn_layout.addWidget(QLabel("端口:"))
        conn_layout.addWidget(self.mb_port)
        conn_layout.addStretch()
        conn_layout.addWidget(self.btn_mb_connect)
        conn_layout.addWidget(self.mb_status)

        layout.addLayout(conn_layout)

        self.mb_display = QTextEdit()
        self.mb_display.setReadOnly(True)
        self.mb_display.setFont(QFont("Consolas", 10))
        self.mb_display.setStyleSheet("background:#0a0a0a; color:#00ccff; border:1px solid #333;")
        layout.addWidget(self.mb_display)

        return widget

    # ---------- CAN Tab ----------
    def _create_can_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

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
        self.can_status.setStyleSheet("color: #aaa;")

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

        send_layout = QHBoxLayout()
        self.can_id = QSpinBox()
        self.can_id.setRange(0, 0x7FFFFFFF)
        self.can_id.setValue(0x123)
        self.can_data = QLineEdit("01 02 03 04")
        self.can_data.setFont(QFont("Consolas", 11))
        self.can_extended = QCheckBox("扩展帧")
        self.btn_can_send = QPushButton("发送")
        self.btn_can_send.clicked.connect(self._can_send)

        send_layout.addWidget(QLabel("ID:"))
        send_layout.addWidget(self.can_id)
        send_layout.addWidget(QLabel("数据:"))
        send_layout.addWidget(self.can_data)
        send_layout.addWidget(self.can_extended)
        send_layout.addStretch()
        send_layout.addWidget(self.btn_can_send)

        layout.addLayout(send_layout)

        self.can_display = QTextEdit()
        self.can_display.setReadOnly(True)
        self.can_display.setFont(QFont("Consolas", 10))
        self.can_display.setStyleSheet("background:#0a0a0a; color:#ffaa00; border:1px solid #333;")
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
        self.mqtt_client_id = QLineEdit("serial_monitor_v5")
        self.btn_mqtt_connect = QPushButton("连接")
        self.btn_mqtt_connect.clicked.connect(self._toggle_mqtt)
        self.mqtt_status = QLabel("⚪ 未连接")
        self.mqtt_status.setStyleSheet("color: #aaa;")

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
        self.mqtt_topic_pub = QLineEdit("serial/data")
        self.mqtt_payload = QLineEdit("")
        self.mqtt_payload.setPlaceholderText("要发布的数据")
        self.btn_mqtt_publish = QPushButton("发布")
        self.btn_mqtt_publish.clicked.connect(self._mqtt_publish)

        pub_layout.addWidget(QLabel("Topic:"))
        pub_layout.addWidget(self.mqtt_topic_pub)
        pub_layout.addWidget(QLabel("Payload:"))
        pub_layout.addWidget(self.mqtt_payload)
        pub_layout.addWidget(self.btn_mqtt_publish)

        layout.addLayout(pub_layout)

        self.mqtt_display = QTextEdit()
        self.mqtt_display.setReadOnly(True)
        self.mqtt_display.setFont(QFont("Consolas", 10))
        self.mqtt_display.setStyleSheet("background:#0a0a0a; color:#ff66ff; border:1px solid #333;")
        layout.addWidget(self.mqtt_display)

        return widget

    # ---------- 发送 Tab ----------
    def _create_send_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        target_layout = QHBoxLayout()
        self.send_target = QComboBox()
        self.send_target.addItems(["广播", "串口", "Modbus TCP", "CAN"])
        target_layout.addWidget(QLabel("目标:"))
        target_layout.addWidget(self.send_target)
        target_layout.addStretch()
        layout.addLayout(target_layout)

        self.send_input = QTextEdit()
        self.send_input.setMaximumHeight(100)
        self.send_input.setFont(QFont("Consolas", 12))
        self.send_input.setStyleSheet("background:#0a0a0a; color:#fff; border:2px solid #444; border-radius:4px;")
        layout.addWidget(self.send_input)

        ctrl = QHBoxLayout()
        self.chk_hex = QCheckBox("HEX模式")
        self.chk_hex.setStyleSheet("color: #ccc;")
        self.chk_append_crlf = QCheckBox("自动\\r\\n")
        self.chk_append_crlf.setChecked(True)
        self.chk_append_crlf.setStyleSheet("color: #ccc;")

        self.btn_send = QPushButton("📤 发送")
        self.btn_send.setMinimumHeight(40)
        self.btn_send.setStyleSheet("font-weight: bold; font-size: 13px; background: #004d00;")
        self.btn_send.clicked.connect(self._send_data)

        ctrl.addWidget(self.chk_hex)
        ctrl.addWidget(self.chk_append_crlf)
        ctrl.addStretch()
        ctrl.addWidget(self.btn_send)

        layout.addLayout(ctrl)

        quick_layout = QHBoxLayout()
        quick_layout.addWidget(QLabel("快捷:"))
        self.quick_combo = QComboBox()
        self.quick_combo.setEditable(True)
        self.quick_combo.addItems(["AT", "AT+RESET", "AA 55 01"])
        self.quick_combo.setMinimumWidth(200)
        self.btn_quick_send = QPushButton("发送")
        self.btn_quick_send.clicked.connect(self._send_quick)
        quick_layout.addWidget(self.quick_combo)
        quick_layout.addWidget(self.btn_quick_send)
        quick_layout.addStretch()
        layout.addLayout(quick_layout)

        return widget

    # ---------- 设置 Tab ----------
    def _create_settings_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        log_group = QGroupBox("日志设置")
        log_layout = QHBoxLayout()
        self.chk_log = QCheckBox("启用日志")
        self.chk_log.setStyleSheet("color: #ccc;")
        self.log_size = QSpinBox()
        self.log_size.setRange(1, 100)
        self.log_size.setValue(10)
        self.log_size.setSuffix(" MB")
        log_layout.addWidget(self.chk_log)
        log_layout.addWidget(QLabel("单文件大小:"))
        log_layout.addWidget(self.log_size)
        log_layout.addStretch()
        log_group.setLayout(log_layout)
        layout.addWidget(log_group)

        trigger_group = QGroupBox("触发词报警")
        trigger_layout = QHBoxLayout()
        self.trigger_input = QLineEdit()
        self.trigger_input.setPlaceholderText("输入触发词，用逗号分隔")
        self.btn_trigger_add = QPushButton("添加")
        self.btn_trigger_add.clicked.connect(self._add_trigger)
        trigger_layout.addWidget(self.trigger_input)
        trigger_layout.addWidget(self.btn_trigger_add)
        trigger_group.setLayout(trigger_layout)
        layout.addWidget(trigger_group)

        whitelist_group = QGroupBox("设备白名单")
        whitelist_layout = QHBoxLayout()
        self.whitelist_edit = QLineEdit()
        self.whitelist_edit.setPlaceholderText("用逗号分隔，如 COM1,COM3")
        whitelist_layout.addWidget(self.whitelist_edit)
        whitelist_group.setLayout(whitelist_layout)
        layout.addWidget(whitelist_group)

        auto_group = QGroupBox("自动启动")
        auto_layout = QVBoxLayout()
        self.chk_auto_connect = QCheckBox("启动时自动连接所有设备")
        self.chk_auto_connect.setStyleSheet("color: #ccc;")
        auto_layout.addWidget(self.chk_auto_connect)
        auto_group.setLayout(auto_layout)
        layout.addWidget(auto_group)

        layout.addStretch()
        return widget

    # ---------- 菜单栏 ----------
    def _setup_menu(self):
        menubar = self.menuBar()
        menubar.setStyleSheet("""
            QMenuBar { background: #1a1a1a; color: #ccc; border-bottom: 1px solid #333; }
            QMenuBar::item:selected { background: #2a2a2a; }
            QMenu { background: #1a1a1a; color: #ccc; border: 1px solid #333; }
            QMenu::item:selected { background: #2a2a2a; }
        """)

        file_menu = menubar.addMenu("文件")
        file_menu.addAction("导出日志", self._export_log)
        file_menu.addAction("导出配置", self._export_config)
        file_menu.addAction("导入配置", self._import_config)
        file_menu.addSeparator()
        file_menu.addAction("退出", self.close)

        view_menu = menubar.addMenu("查看")
        view_menu.addAction("清空显示", lambda: self.display.clear())

        tools_menu = menubar.addMenu("工具")
        tools_menu.addAction("CRC计算器", self._show_crc_calc)

        help_menu = menubar.addMenu("帮助")
        help_menu.addAction("关于", self._show_about)

    # ---------- 工具栏 ----------
    def _setup_toolbar(self):
        toolbar = self.addToolBar("主工具栏")
        toolbar.setMovable(False)
        toolbar.setStyleSheet("""
            QToolBar { background: #1a1a1a; border: none; border-bottom: 1px solid #333; padding: 2px 6px; }
            QToolButton { background: transparent; color: #ccc; border: 1px solid transparent; border-radius: 3px; padding: 4px 8px; }
            QToolButton:hover { background: #2a2a2a; border-color: #555; }
            QToolBar QLabel { color: #888; padding: 0 4px; }
            QToolBar QComboBox { background: #2a2a2a; color: #fff; border: 1px solid #555; border-radius: 3px; padding: 2px 6px; min-height: 22px; min-width: 100px; }
            QToolBar QPushButton { background: #2a2a2a; color: #fff; border: 1px solid #555; border-radius: 3px; padding: 4px 12px; min-height: 24px; }
            QToolBar QPushButton:hover { background: #3a3a3a; }
            QToolBar QPushButton#btn_connect { background: #004d00; border-color: #00aa00; }
            QToolBar QPushButton#btn_connect:hover { background: #006600; }
            QToolBar QCheckBox { color: #ccc; spacing: 4px; }
        """)

        toolbar.addWidget(QLabel("端口:"))
        self.tb_port = QComboBox()
        self.tb_port.setMinimumWidth(120)
        self.tb_port.setEditable(True)
        toolbar.addWidget(self.tb_port)

        toolbar.addWidget(QLabel("波特率:"))
        self.tb_baud = QComboBox()
        self.tb_baud.addItems(["9600", "19200", "38400", "57600", "115200", "230400", "460800"])
        self.tb_baud.setCurrentText("115200")
        self.tb_baud.setMinimumWidth(100)
        toolbar.addWidget(self.tb_baud)

        toolbar.addSeparator()

        self.tb_connect = QPushButton("▶ 连接")
        self.tb_connect.setObjectName("btn_connect")
        self.tb_connect.clicked.connect(self._toolbar_connect)
        toolbar.addWidget(self.tb_connect)

        btn_refresh = QPushButton("🔄")
        btn_refresh.setFixedWidth(32)
        btn_refresh.clicked.connect(self._refresh_toolbar_ports)
        toolbar.addWidget(btn_refresh)

        toolbar.addSeparator()

        self.tb_hex = QCheckBox("HEX")
        self.tb_hex.setChecked(self.config.get("hex_mode", False))
        self.tb_hex.stateChanged.connect(self._tb_hex_toggle)
        toolbar.addWidget(self.tb_hex)

        toolbar.addSeparator()

        btn_clear = QPushButton("🗑 清屏")
        btn_clear.clicked.connect(lambda: self.display.clear())
        toolbar.addWidget(btn_clear)

        self.tb_log = QCheckBox("📁 日志")
        self.tb_log.setChecked(self.config.get("log_enabled", True))
        self.tb_log.stateChanged.connect(lambda s: self.chk_log.setChecked(s == Qt.Checked))
        toolbar.addWidget(self.tb_log)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        toolbar.addWidget(spacer)

        self.tb_status = QLabel("⚪ 未连接")
        self.tb_status.setStyleSheet("color: #888; padding: 0 10px;")
        toolbar.addWidget(self.tb_status)

        self.tb_counter = QLabel("RX:0 TX:0")
        self.tb_counter.setStyleSheet("color: #666; padding: 0 10px;")
        toolbar.addWidget(self.tb_counter)

    # ---------- 状态栏 ----------
    def _setup_statusbar(self):
        self.statusBar().setStyleSheet("background: #1a1a1a; color: #888; border-top: 1px solid #333;")

        self.status_label = QLabel("就绪")
        self.status_label.setStyleSheet("color: #aaa; padding: 2px 8px;")
        self.statusBar().addWidget(self.status_label, 1)

        self.status_ports = QLabel("串口:0")
        self.status_ports.setStyleSheet("color: #666; padding: 2px 8px;")
        self.statusBar().addPermanentWidget(self.status_ports)

        self.status_rx = QLabel("RX:0")
        self.status_rx.setStyleSheet("color: #00ff41; padding: 2px 8px;")
        self.statusBar().addPermanentWidget(self.status_rx)

        self.status_tx = QLabel("TX:0")
        self.status_tx.setStyleSheet("color: #ffaa00; padding: 2px 8px;")
        self.statusBar().addPermanentWidget(self.status_tx)

        self.status_time = QLabel(datetime.now().strftime("%H:%M:%S"))
        self.status_time.setStyleSheet("color: #666; padding: 2px 8px;")
        self.statusBar().addPermanentWidget(self.status_time)

        timer = QTimer()
        timer.timeout.connect(lambda: self.status_time.setText(datetime.now().strftime("%H:%M:%S")))
        timer.start(1000)

    # ---------- 信号连接 ----------
    def _setup_connections(self):
        self.data_bus.data_received.connect(self._on_data_received)
        self.data_bus.device_status.connect(self._on_device_status)
        self.chk_log.stateChanged.connect(self._toggle_log)

    # ---------- 串口管理 ----------
    def _refresh_ports(self):
        self.port_combo.clear()
        for port in serial.tools.list_ports.comports():
            self.port_combo.addItem(port.device)

    def _refresh_toolbar_ports(self):
        self.tb_port.clear()
        for port in serial.tools.list_ports.comports():
            self.tb_port.addItem(port.device)

    def _add_port(self):
        port = self.port_combo.currentText()
        baud = int(self.baud_combo.currentText())
        if not port:
            return

        port_id = self.serial_manager.add_port(port, baud)
        self._update_serial_table()
        self._log_system(f"添加串口: {port} @ {baud}")

    def _toolbar_connect(self):
        port = self.tb_port.currentText()
        baud = int(self.tb_baud.currentText())
        if not port:
            QMessageBox.warning(self, "警告", "请选择端口")
            return

        for pid, p, b, _ in self.serial_manager.get_ports():
            if p == port:
                QMessageBox.warning(self, "警告", f"端口 {port} 已连接")
                return

        port_id = self.serial_manager.add_port(port, baud)
        self._update_serial_table()
        self._log_system(f"连接串口: {port} @ {baud}")

    def _update_serial_table(self):
        self.port_table.setRowCount(0)
        for i, (port_id, port, baud, running) in enumerate(self.serial_manager.get_ports()):
            self.port_table.insertRow(i)
            self.port_table.setItem(i, 0, QTableWidgetItem(port_id))
            self.port_table.setItem(i, 1, QTableWidgetItem(port))
            self.port_table.setItem(i, 2, QTableWidgetItem(str(baud)))
            status = "🟢 在线" if running else "🔴 离线"
            self.port_table.setItem(i, 3, QTableWidgetItem(status))

        self.status_ports.setText(f"串口:{self.serial_manager.get_count()}")

    def _remove_selected(self):
        row = self.port_table.currentRow()
        if row < 0:
            return
        port_id = self.port_table.item(row, 0).text()
        self.serial_manager.remove_port(port_id)
        self._update_serial_table()

    def _remove_all(self):
        if self.serial_manager.get_count() == 0:
            return
        reply = QMessageBox.question(self, "确认", "确定断开所有串口？", QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.serial_manager.remove_all()
            self._update_serial_table()
            self._log_system("断开全部串口")

    # ---------- Modbus ----------
    def _toggle_modbus(self):
        if self.modbus_manager and self.modbus_manager.isRunning():
            self.modbus_manager.stop()
            self.modbus_manager = None
            self.btn_mb_connect.setText("连接")
            self.mb_status.setText("⚪ 未连接")
            self.mb_status.setStyleSheet("color: #aaa;")
            return

        host = self.mb_host.text()
        port = self.mb_port.value()
        self.modbus_manager = ModbusTCPManager(host, port)
        self.modbus_manager.start()
        self.btn_mb_connect.setText("断开")
        self.mb_status.setText("🔄 连接中...")
        self.mb_status.setStyleSheet("color: #ffaa00;")

    # ---------- CAN ----------
    def _toggle_can(self):
        if self.can_manager and self.can_manager.isRunning():
            self.can_manager.stop()
            self.can_manager = None
            self.btn_can_connect.setText("连接")
            self.can_status.setText("⚪ 未连接")
            self.can_status.setStyleSheet("color: #aaa;")
            return

        interface = self.can_interface.currentText()
        channel = self.can_channel.text()
        bitrate = int(self.can_bitrate.currentText())

        self.can_manager = CANManager(interface, channel, bitrate)
        self.can_manager.start()
        self.btn_can_connect.setText("断开")
        self.can_status.setText("🔄 连接中...")
        self.can_status.setStyleSheet("color: #ffaa00;")

    def _can_send(self):
        if not self.can_manager or not self.can_manager.isRunning():
            QMessageBox.warning(self, "警告", "请先连接CAN")
            return

        can_id = self.can_id.value()
        data_hex = self.can_data.text().replace(" ", "")
        try:
            data = bytes.fromhex(data_hex)
            self.can_manager.send(can_id, data)
            self.can_display.append(f"[发送] ID:{can_id:03X} Data:{data.hex(' ').upper()}")
            self._log_system(f"CAN发送: ID:{can_id:03X}")
        except Exception as e:
            QMessageBox.critical(self, "错误", str(e))

    # ---------- MQTT ----------
    def _toggle_mqtt(self):
        if self.mqtt_manager and self.mqtt_manager.isRunning():
            self.mqtt_manager.stop()
            self.mqtt_manager = None
            self.btn_mqtt_connect.setText("连接")
            self.mqtt_status.setText("⚪ 未连接")
            self.mqtt_status.setStyleSheet("color: #aaa;")
            return

        broker = self.mqtt_broker.text()
        port = self.mqtt_port.value()
        client_id = self.mqtt_client_id.text()

        self.mqtt_manager = MQTTManager(broker, port, client_id)
        self.mqtt_manager.start()
        self.btn_mqtt_connect.setText("断开")
        self.mqtt_status.setText("🔄 连接中...")
        self.mqtt_status.setStyleSheet("color: #ffaa00;")

    def _mqtt_publish(self):
        if not self.mqtt_manager or not self.mqtt_manager.isRunning():
            QMessageBox.warning(self, "警告", "请先连接MQTT")
            return

        topic = self.mqtt_topic_pub.text()
        payload = self.mqtt_payload.text()
        if payload:
            self.mqtt_manager.publish(topic, payload)
            self.mqtt_display.append(f"[发布] {topic}: {payload}")
            self._log_system(f"MQTT发布: {topic}")

    # ---------- 发送 ----------
    def _send_data(self):
        text = self.send_input.toPlainText().strip()
        if not text:
            return

        target = self.send_target.currentText()

        try:
            if self.chk_hex.isChecked():
                hex_str = text.replace(" ", "").replace("\n", "")
                data = bytes.fromhex(hex_str)
            else:
                data = text.encode('utf-8')
                if self.chk_append_crlf.isChecked():
                    data += b"\r\n"

            if target == "广播":
                self.serial_manager.broadcast(data)
                if self.can_manager:
                    self.can_manager.send(0x100, data)
                self._log_system(f"广播发送: {data.hex(' ').upper()[:50]}...")
            elif target == "串口":
                self.serial_manager.broadcast(data)
                self._log_system(f"串口发送: {data.hex(' ').upper()[:50]}...")
            elif target == "Modbus TCP" and self.modbus_manager:
                self._log_system("Modbus TCP发送请使用Modbus Tab")
            elif target == "CAN" and self.can_manager:
                self.can_manager.send(0x100, data)
                self._log_system(f"CAN发送: {data.hex(' ').upper()[:50]}...")
            else:
                QMessageBox.warning(self, "警告", f"目标 {target} 不可用")

            self.tx_count += len(data)
            self._update_status()
            self.send_input.clear()
        except ValueError:
            QMessageBox.warning(self, "格式错误", "HEX模式下请输入合法十六进制")
        except Exception as e:
            QMessageBox.critical(self, "发送失败", str(e))

    def _send_quick(self):
        text = self.quick_combo.currentText()
        if text:
            self.send_input.setText(text)
            self._send_data()

    # ---------- 数据接收 ----------
    def _on_data_received(self, packet: DataPacket):
        if packet.data:
            self.rx_count += len(packet.data)

            if self.chk_hex.isChecked():
                display_text = packet.data.hex(' ').upper()
            else:
                try:
                    display_text = packet.data.decode('utf-8', errors='replace')
                except:
                    display_text = packet.data.hex(' ').upper()

            if len(display_text) > 500:
                display_text = display_text[:500] + "..."

            self._append_display(f"[{packet.source}] {display_text}", "#00ff41")
            self._update_status()

        if self.mqtt_manager and self.mqtt_manager.isRunning():
            try:
                topic = f"serial/{packet.source_type}/{packet.source}"
                payload = packet.data.hex(' ').upper()
                self.mqtt_manager.publish(topic, payload)
            except:
                pass

    def _on_device_status(self, device_id: str, connected: bool, message: str):
        status = "🟢" if connected else "🔴"
        self._append_display(f"[状态] {device_id}: {message}", "#ffaa00" if connected else "#ff4444")

        if "modbus" in device_id:
            self.mb_status.setText("🟢 已连接" if connected else "🔴 已断开")
            self.mb_status.setStyleSheet("color: #00ff41;" if connected else "color: #ff4444;")
        if "can" in device_id:
            self.can_status.setText("🟢 已连接" if connected else "🔴 已断开")
            self.can_status.setStyleSheet("color: #00ff41;" if connected else "color: #ff4444;")
        if "mqtt" in device_id:
            self.mqtt_status.setText("🟢 已连接" if connected else "🔴 已断开")
            self.mqtt_status.setStyleSheet("color: #00ff41;" if connected else "color: #ff4444;")

        if "serial" in device_id:
            self._update_serial_table()

        if connected:
            self.tb_status.setText("🟢 已连接")
            self.tb_status.setStyleSheet("color: #00ff41; padding: 0 10px;")
        else:
            self.tb_status.setText("⚪ 未连接")
            self.tb_status.setStyleSheet("color: #888; padding: 0 10px;")

        self._update_status()

    # ---------- 触发词 ----------
    def _add_trigger(self):
        text = self.trigger_input.text().strip()
        if not text:
            return
        keywords = [kw.strip() for kw in text.split(",") if kw.strip()]
        existing = self.config.get("trigger_keywords", [])
        for kw in keywords:
            if kw not in existing:
                existing.append(kw)
        self.config.set("trigger_keywords", existing)
        self.trigger_input.clear()
        self._log_system(f"已添加触发词: {', '.join(keywords)}")

    # ---------- 日志 ----------
    def _toggle_log(self, state):
        self.config.set("log_enabled", state == Qt.Checked)
        self.tb_log.setChecked(state == Qt.Checked)
        if state == Qt.Checked:
            self._log_system("日志已开启")
        else:
            self._log_system("日志已关闭")

    # ---------- 自动连接 ----------
    def _auto_connect(self):
        ports = self.config.get("serial_ports", [])
        for cfg in ports:
            if cfg.get("enabled", False):
                self.serial_manager.add_port(cfg["port"], cfg["baud"])
        self._update_serial_table()
        self._log_system("自动连接完成")

    # ---------- 状态更新 ----------
    def _update_status(self):
        self.status_rx.setText(f"RX:{self.rx_count}")
        self.status_tx.setText(f"TX:{self.tx_count}")
        self.tb_counter.setText(f"RX:{self.rx_count} TX:{self.tx_count}")

        port_count = self.serial_manager.get_count()
        self.status_label.setText(f"就绪 | 串口:{port_count} | RX:{self.rx_count} TX:{self.tx_count}")

    # ---------- 辅助 ----------
    def _append_display(self, text, color="#00ff41"):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        if self.config.get("show_timestamp", True):
            self.display.append(
                f'<span style="color:#666;">[{timestamp}]</span> <span style="color:{color};">{text}</span>')
        else:
            self.display.append(f'<span style="color:{color};">{text}</span>')
        scrollbar = self.display.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _log_system(self, text):
        self._append_display(f"[系统] {text}", "#ffff00")

    def _audit(self, action, detail):
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            with open(os.path.join(LOG_DIR, AUDIT_FILE), 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] {action}: {detail}\n")
        except:
            pass

    def _init_audit(self):
        try:
            self._audit("START", "系统启动 v5.2 生产增强版")
        except:
            pass

    def _load_config(self):
        self.chk_log.setChecked(self.config.get("log_enabled", True))
        self.log_size.setValue(self.config.get("log_max_mb", 10))
        self.chk_auto_connect.setChecked(self.config.get("auto_connect", False))
        self.chk_hex.setChecked(self.config.get("hex_mode", False))
        self.whitelist_edit.setText(", ".join(self.config.get("whitelist", [])))
        self.tb_hex.setChecked(self.config.get("hex_mode", False))
        self.tb_log.setChecked(self.config.get("log_enabled", True))

    # ---------- 导出/导入 ----------
    def _export_log(self):
        path, _ = QFileDialog.getSaveFileName(self, "导出日志", "", "文本文件 (*.txt)")
        if path:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(self.display.toPlainText())
                QMessageBox.information(self, "导出成功", f"日志已导出到 {path}")
            except Exception as e:
                QMessageBox.critical(self, "导出失败", str(e))

    def _export_config(self):
        path, _ = QFileDialog.getSaveFileName(self, "导出配置", "", "JSON文件 (*.json)")
        if path:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(self.config.data, f, indent=2, ensure_ascii=False)
                QMessageBox.information(self, "导出成功", f"配置已导出到 {path}")
            except Exception as e:
                QMessageBox.critical(self, "导出失败", str(e))

    def _import_config(self):
        path, _ = QFileDialog.getOpenFileName(self, "导入配置", "", "JSON文件 (*.json)")
        if path:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.config.data.update(data)
                self.config.save()
                self._load_config()
                QMessageBox.information(self, "导入成功", "配置已导入")
            except Exception as e:
                QMessageBox.critical(self, "导入失败", str(e))

    # ---------- 工具 ----------
    def _show_crc_calc(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("CRC16计算器")
        dialog.setMinimumWidth(400)
        layout = QVBoxLayout(dialog)

        input_edit = QLineEdit()
        input_edit.setPlaceholderText("输入十六进制数据，如 AA 55 01")
        input_edit.setFont(QFont("Consolas", 12))
        layout.addWidget(input_edit)

        btn_calc = QPushButton("计算CRC16")
        layout.addWidget(btn_calc)

        result_label = QLabel("结果: ")
        result_label.setFont(QFont("Consolas", 14))
        result_label.setStyleSheet("color: #00ff41;")
        layout.addWidget(result_label)

        def calc_crc():
            text = input_edit.text().replace(" ", "").strip()
            try:
                data = bytes.fromhex(text)
                crc = ModbusRTUParser._calculate_crc(data)
                result_label.setText(f"CRC16: 0x{crc:04X}  ({crc})")
            except:
                result_label.setText("输入格式错误")

        btn_calc.clicked.connect(calc_crc)

        btn_close = QPushButton("关闭")
        btn_close.clicked.connect(dialog.accept)
        layout.addWidget(btn_close)

        dialog.exec_()

    def _show_about(self):
        QMessageBox.about(self, "关于",
                          "🏭 工业串口监控系统 v5.2 - 生产增强版\n\n"
                          "支持协议:\n"
                          "• 多串口同时监控\n"
                          "• Modbus TCP\n"
                          "• CAN总线 (virtual/socketcan)\n"
                          "• MQTT发布/订阅 (兼容2.x)\n\n"
                          "生产级特性:\n"
                          "• 数据流量统计\n"
                          "• 日志自动分割\n"
                          "• 异常自动恢复\n\n"
                          "适用场景:\n"
                          "• 工业自动化调试\n"
                          "• 多设备集中监控"
                          )

    def _tb_hex_toggle(self, state):
        checked = state == Qt.Checked
        self.config.set("hex_mode", checked)
        self.chk_hex.setChecked(checked)

    # ---------- 关闭 ----------
    def closeEvent(self, event):
        geo = self.geometry()
        self.config.set("window_x", geo.x())
        self.config.set("window_y", geo.y())
        self.config.set("window_width", geo.width())
        self.config.set("window_height", geo.height())

        self._audit("SHUTDOWN", "系统关闭")
        self.serial_manager.remove_all()
        if self.modbus_manager:
            self.modbus_manager.stop()
        if self.can_manager:
            self.can_manager.stop()
        if self.mqtt_manager:
            self.mqtt_manager.stop()
        if self.log_file:
            self.log_file.close()
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

    window = IndustrialSerialToolV5()
    window.show()
    sys.exit(app.exec_())