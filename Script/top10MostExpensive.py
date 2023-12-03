import os
import sys
from top10MostExpensiveUS import top10MostExpensiveUS
from top10MostExpensiveChina import top10MostExpensiveChina
from top10MostExpensiveIndia import top10MostExpensiveIndia
from matplotlib import pyplot as plt


# Usage: spark-submit top10MostExpensive.py <dataset dir>

def top10MostExpensive(dir):
    result = top10MostExpensiveUS(dir + "/US_data")
    result += top10MostExpensiveChina(dir + "/China_data")
    result += top10MostExpensiveIndia(dir + "/India_data")

    result = sorted(result, key=lambda x : x[1], reverse=True)

    return result[:10]

def generateImg(result, path = "./"):
    Company, Price = zip(*result)
    plt.bar(Company, Price)
    plt.xlabel('Company')
    plt.ylabel('Max Price')
    plt.title('top10MostExpensive')
    plt.xticks(rotation=45, ha="right")
    plt.savefig(os.path.join(path, 'top10MostExpensive.png'))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit top10MostExpensive.py <dataset dir>")

    result = top10MostExpensive(sys.argv[1])
    print(result)

    if len(sys.argv) > 2:
        generateImg(result,sys.argv[2])
    else:
        generateImg(result)
    