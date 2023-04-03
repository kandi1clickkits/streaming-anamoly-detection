from kafka import KafkaConsumer, KafkaProducer
import json
import pickle
from json import loads
import numpy as np
import sys
import time
import os
import warnings
warnings.filterwarnings("ignore")

#model_path = os.path.abspath(r".\model.pickle")

#with open(model_path, 'rb') as f:
#    clf = pickle.load(f)

with open('model.pickle', 'rb') as f:
    clf = pickle.load(f)

bootstrap_servers = ['localhost:9092']
topicname = 'potential-drowning'

consumer = KafkaConsumer(topicname, bootstrap_servers=bootstrap_servers, auto_offset_reset = 'latest', value_deserializer= lambda x: loads(x.decode('utf-8')))

try:
    for message in consumer:
        time.sleep(1)
        record = json.loads(json.dumps(message.value))
        
        data = list(record.values())        
        prediction = clf.predict([data])        
        if prediction[0] == -1:
            record['potentialDrowning'] = 1
        else: record['potentialDrowning'] = 0
        
        score = -1*clf.score_samples([data])
        record["score"] = np.round(score[0], 3)
        print(record)
    consumer.close()

except KeyboardInterrupt:
    sys.exit()
