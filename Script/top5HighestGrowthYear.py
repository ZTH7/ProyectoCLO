import os
import sys
from top5HighestGrowthUSYear import top5HighestGrowthUSYear
from top5HighestGrowthChinaYear import top5HighestGrowthChinaYear
from top5HighestGrowthIndiaYear import top5HighestGrowthIndiaYear

def top5HighestGrowthYear(dir,year):
    result=[]
    result.extend(top5HighestGrowthUSYear(dir + "/US_data",year))
    result.extend(top5HighestGrowthChinaYear(dir + "/China_data",year))
    result.extend(top5HighestGrowthIndiaYear(dir + "/India_data",year))

    result = sorted(result, key=lambda x : x[1], reverse=True)

    return result[:10]


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit top5HighestGrowthYear.py <dataset dir> <year>")
    top5HighestGrowthYear(sys.argv[1],sys.argv[2])
