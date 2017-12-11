# 说明文档

包括 Mapreduce 与 UDF 代码，以及测试类

## 包内容说明

| 包名 | 内容 |
| :----: | :----: |
| com.legends.mr   | Mapreduce 类 |
| com.legends.main | Mapreduce 运行测试类 |
| com.legends.udf  | UDF 类 |

## Mapreduce 类

相应功能类分别以 Map 和 Reduce 结尾，只有 Map 的类以 OnlyMap 结尾
 
| 名字 | 功能 |
| :----: | :----: |
| SplitBehaviorWifi | 单 Map，切分 wifi_infos 为多行 |
| RestoreWifi | 将 多行 wifi 变为 一行 |
| DistanceShop   | 输入两个经纬度，计算最小距离 |
| ShopAppearNum | 计算 shop 人流量 |
| ShopAvgAssi | 计算 shop 的 wifi 平均强度 |
| ShopWifiGroup | wifi_infos 中 bssid 取出，输出  shop_id,(b1-b2-b3....)  |

## UDF 类
 
| 名字 | 功能 |
| :----: | :----: |
| GetJsonKV | 解析多分类输出结果的 prediction_detail Json 格式，并展开为多行 UDTF  |
| SpherDistance | 将 多行 wifi 变为 一行 |
| DistanceShop   | 计算球面距离的 UDF |
| SplitTail | 简单切分 '_'，返回 bigint |