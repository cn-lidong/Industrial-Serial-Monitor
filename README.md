# 🏭 工业串口监控系统 v6.0

一款面向工业自动化领域的**全功能串口/总线调试工具**；开箱即用。

---

## ✨ 核心功能

| 功能模块 | 说明 |
|----------|------|
| 🔌 **串口监控** | 多通道并行监控、自动重连、超时重发 |
| 📶 **Modbus RTU** | 串口 Modbus 协议 (03/06/16 功能码) |
| 📶 **Modbus TCP** | 网络 Modbus 协议 (03/06 功能码) |
| 🚗 **CAN 总线** | 标准帧/扩展帧收发 |
| 🚗 **CANopen** | NMT/SDO/PDO 基础协议支持 |
| 📡 **MQTT** | 发布/订阅 (兼容 2.x API) |
| 🔗 **OPC UA** | 客户端读写 (支持 asyncua) |
| 📊 **实时曲线** | 数据趋势图 (PyQtChart) |
| 🤖 **脚本自动化** | Python 引擎 + 定时器/事件触发 |
| 🔄 **多设备轮询** | 定时轮询多台 Modbus 设备 |
| 📤 **通用发送** | HEX/文本双模式、多目标广播 |
| ⚙️ **运维辅助** | 日志记录、触发词报警、CRC16 计算 |

---

## 🖥️ 界面预览

<img width="1394" height="934" alt="image" src="https://github.com/user-attachments/assets/db996a05-d441-4d52-8ad9-043d443caadb" />

---

## 🚀 快速开始

### 下载 exe 直接运行

超过25M无法上传，你编译成exe可执行文件吧

### 从源码运行

```bash
# 1. 克隆仓库
git clone https://github.com/cn-lidong/Industrial-Serial-Monitor.git
cd Industrial-Serial-Monitor

# 2. 安装依赖
pip install -r requirements.txt

# 3. 运行
python comtest60.py

📦 依赖安装
bash
pip install PyQt5 PyQtChart pyserial pymodbus python-can paho-mqtt asyncua
🔧 打包为 exe
bash
pyinstaller -F -w --name="工业串口监控系统v6.0" comtest60.py
📁 项目结构
text
Industrial-Serial-Monitor/
├── comtest60.py              # 主程序
├── requirements.txt          # 依赖清单
├── README.md                 # 项目说明
├── screenshot.png            # 界面截图
└── dist/
    └── 工业串口监控系统v6.0.exe   # 打包后的 exe
🎯 适用场景
场景	说明
PLC 调试	Modbus RTU/TCP 读写寄存器
传感器数据采集	串口/Modbus 读取温度、压力、流量
车载 CAN 诊断	CAN + CANopen 协议分析
物联网网关	串口/CAN → MQTT 数据上云
产线集中监控	多设备轮询 + 触发词报警
自动化测试	Python 脚本控制设备批量测试
🛠️ 技术栈
技术	用途
Python 3.12	开发语言
PyQt5	GUI 框架
PyQtChart	实时曲线
pyserial	串口通信
pymodbus	Modbus 协议
python-can	CAN 总线
paho-mqtt	MQTT 协议
asyncua	OPC UA 客户端
📜 许可证
MIT License

🤝 贡献
欢迎提交 Issue 和 Pull Request！

📧 联系方式
GitHub: cn-lidong

邮箱: 137171666@qq.com
