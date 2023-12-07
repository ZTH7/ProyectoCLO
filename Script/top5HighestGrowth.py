import os
import sys
from top5HighestGrowthUS import top5HighestGrowthUS
from top5HighestGrowthChina import top5HighestGrowthChina
from top5HighestGrowthIndia import top5HighestGrowthIndia

def top5HighestGrowth(dir):
    result=[]
    result.extend(top5HighestGrowthUS(dir + "/US_data"))
    result.extend(top5HighestGrowthChina(dir + "/China_data"))
    result.extend(top5HighestGrowthIndia(dir + "/India_data"))

    result = sorted(result, key=lambda x : x[1], reverse=True)

    return result[:10]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit top5HighestGrowth.py <dataset dir>")
    print(top5HighestGrowth(sys.argv[1]))
