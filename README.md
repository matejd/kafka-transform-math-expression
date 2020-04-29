# MathExpression
A Kafka Single Message Transform (SMT) that evaluates simple mathematical expressions.

https://kafka.apache.org/documentation/#connect_transforms

## Config

SMT expects the following configuration values:
- `expression` (see https://github.com/uklimaschewski/EvalEx, record fields can be used as variables)
- `field.name` (evaluation result will be stored in this field)
- `field.type` (can be used to cast evaluation result to a different type, defaults to string)

Supported field types: `int64`, `int64?` (nullable), `float64`, `float64?`, `string` (default), `string?`.

Example configuration that divides field `timestamp` by 1000:

```
"transforms": "math",
"transforms.math.type": "com.github.matejd.kafka.connect.transform.MathExpression$Value",
"transforms.math.expression": "timestamp / 1000",
"transforms.math.field.name": "timestamp_seconds",
"transforms.math.field.type": "int64?",
```
