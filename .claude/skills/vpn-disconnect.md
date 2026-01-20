# VPN Disconnect

会社VPN（L2TP/IPsec）を切断する

## 手順

```bash
# 1. L2TP接続を切断
sudo bash -c 'echo "d ohishi" > /var/run/xl2tpd/l2tp-control'

# 2. IPsec接続を切断
sudo ipsec down ohishi

# 3. 切断確認
ip addr show ppp0 2>/dev/null || echo "VPN disconnected"
```
