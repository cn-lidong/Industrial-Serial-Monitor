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
