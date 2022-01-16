# Overview

# Running locally

Assuming you already have a Kafka cluster running locally and a topic with data, you can consume (basic printing to STDOUT) with the following commands

```
$ python3 s3_archiver/archiver.py --topic tweets-stream
tweets-stream:0:15518: key=None value={'data': {'id': '1482600921771257856', 'text': '@97Abdulmalik I honestly appreciated thier understanding. 👍'}, 'matching_rules': [{'id': '1482581065894039552', 'tag': 'Sampled tweets about feelings, in English'}]}
tweets-stream:0:15519: key=None value={'data': {'id': '1482600926405808128', 'text': '@Crypto__Talk @tuchx2k10 @XRshib agree'}, 'matching_rules': [{'id': '1482581065894039552', 'tag': 'Sampled tweets about feelings, in English'}]}
tweets-stream:0:15520: key=None value={'data': {'id': '1482600929753014275', 'text': '@kirsteincrybaby @itstimetotwt the typo makes this so much funnier'}, 'matching_rules': [{'id': '1482581065894039552', 'tag': 'Sampled tweets about feelings, in English'}]}
```

# Development
