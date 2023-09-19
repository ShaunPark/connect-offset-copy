# 카프카 커넥트 오프셋 조회 및 복사
- 카프카 커넥트의 오프셋 토픽에서 커넥터 목록 조회
- 특정 커넥터의 offset정보를 확인 
- 특정 커넥터의 offset을 다른 커넥터의 offset으로 복사 
- 변경하고자 하는 컨디션이 없으면 해당 컨디션을 추가함
- 컨디션 타입이 정의되지 않으면 모든 컨디션의 상태를 일괄 변경. 단, True로 일괄변경은 불가.

## 사용법
```
usage: java -jar app-0.0.1.jar -p <CONFIG_FILE_NAME> -o <CONNECT_OFFSET_TOPIC> <Options>
 -c,--copy <FROM_CONNECTOR_KEY>             Copy offset of 'from connector' as a offset of 'to connector'.
 -g,--get <CONNECTOR_NAME>                  Get a connector's offset from a connect offset topic with a connector name.
 -l,--list                                  List all connector name in the offset topic.
 -o,--offset-topic <CONNECT_OFFSET_TOPIC>   Connector cluster offset topic name
 -p,--config-file <CONFIG_FILE_NAME>        Config file to connect kafka broker.
 -t,--to-connector <TO_CONNECTOR_KEY>       Name of a destination connector
```

