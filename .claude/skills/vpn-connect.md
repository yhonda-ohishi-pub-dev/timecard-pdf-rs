# VPN Connect

会社VPN（L2TP/IPsec）に接続する

## 手順

以下のコマンドを順番に実行する：

```bash
# 1. IPsec/L2TPサービス再起動
sudo ipsec restart && sleep 2 && sudo systemctl restart xl2tpd

# 2. IPsec接続確立
sudo ipsec up ohishi

# 3. L2TP接続開始
sudo bash -c 'echo "c ohishi" > /var/run/xl2tpd/l2tp-control'

# 4. ppp0確認（3秒待機）
sleep 3 && ip addr show ppp0

# 5. ルート追加
sudo ip route add 172.18.21.0/24 dev ppp0

# 6. 接続確認
ping -c 1 172.18.21.35
```

## 接続情報
- IPsec接続名: ohishi
- VPNサーバー: 114.172.188.66
- 内部IP: 172.18.21.2
- DBサーバー: 172.18.21.35
