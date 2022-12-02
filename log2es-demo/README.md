
### 验证方法

```
GET logback-2022-05/_search
{
  "size": 100, 
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "app.keyword": "app"
          }
        },
        {
          "match": {
            "host.keyword": "DESKTOP-59OIUQS"
          }
        }
      ]
    }
  },
  "sort": [
    {
      "@timestamp": {
        "order": "desc"
      }
    }
  ]
}
```