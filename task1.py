from pyspark import SparkContext
import json
import itertools
import time
import sys
from collections import defaultdict
import random

num_hash=50
bands=50
rows=1
buckets=1000

def main(inFile,outFile):
    def generate_random_num(n):
        random_list=[]
        range_max=n
        while n>0:
            temp=random.randint(1,26184)
            while temp in random_list:
                temp=random.randint(1,26184)
            random_list.append(temp)
            n-=1
        return random_list
    def minhash_list(x):
        profiles=set(x[1])
        final_list=[]
        buckets_belong=[]
        for i in range(0,num_hash):
            id_list=[]
            for profile in profiles:
                new_id=user_to_index[profile][i]
                id_list.append(new_id)
            final_list.append(min(id_list))
        for i,band in enumerate(chunks(final_list,rows)):
            buckets_belong.append((i,band[0]))
            # bucket=(hash(tuple(band))*band_a_list[i]+band_b_list[i])%buckets
            # buckets_belong.append(bucket+i*buckets)
        return [(j,x[0]) for j in buckets_belong]
            
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def jaccard_similarity(x):
        set_1 = set(business_dict[x[0]])
        set_2 = set(business_dict[x[1]])
        return (x,float(len(set_1.intersection(set_2)))/float(len(set_1.union(set_2))))
    
    def hash_fun(x):
        hash_x=user_id[x]
        minhash=[]
        for i in range(num_hash):
            minhash.append((a_list[i]*hash_x+b_list[i])%10253)
        return (x,minhash)

    random.seed(20)
    a_list=generate_random_num(num_hash)
    random.seed(70)
    b_list=generate_random_num(num_hash)
    # band_a_list=generate_random_num(bands)
    # band_b_list=generate_random_num(bands)

    sc = SparkContext(appName="task1")
    lines = sc.textFile(inFile)
    sc.setLogLevel("ERROR")
    user_temp=lines.map(lambda x:json.loads(x)).map(lambda x:x["user_id"]).distinct().cache()
    # business_id=business_temp.map(lambda x: (1,x)).groupByKey().map(lambda x: dict(zip(x[1],list(range(len(x[1])))))).collect()[0]
    user_id=dict(zip(user_temp.collect(),list(range(len(user_temp.collect())))))
    
    user=user_temp.map(hash_fun).collect()

    user_to_index=dict(user)

    temp=lines.map(lambda x: json.loads(x)).map(lambda x: (x["business_id"],x["user_id"])).distinct().groupByKey().cache()

    business_dict=dict(temp.collect())

    business_set=temp.map(minhash_list).flatMap(lambda x:x).groupByKey().flatMap(lambda x:itertools.combinations(sorted(x[1]),2)).distinct()
    
    result= business_set.map(jaccard_similarity).filter(lambda x:x[1]>=0.05).collect()
    with open(outFile,"w") as f:
        for i in result:
            temp_dict={}
            temp_dict["b1"]=i[0][0]
            temp_dict["b2"]=i[0][1]
            temp_dict["sim"]=i[1]
            json.dump(temp_dict,f)
            f.write('\n')

if __name__ =="__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    start_time=time.time()
    main(input_file,output_file)
    print(time.time()-start_time)