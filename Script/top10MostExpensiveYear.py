import os
import sys
from top10MostExpensiveUSYear import top10MostExpensiveUSYear
from top10MostExpensiveChinaYear import top10MostExpensiveChinaYear
from top10MostExpensiveIndiaYear import top10MostExpensiveIndiaYear
from matplotlib import pyplot as plt


# Usage: spark-submit top10MostExpensiveYear.py <dataset dir> <year>

def top10MostExpensiveYear(dir, year):
    result = top10MostExpensiveUSYear(dir + "/US_data", year)
    result += top10MostExpensiveChinaYear(dir + "/China_data", year)
    result += top10MostExpensiveIndiaYear(dir + "/India_data", year)

    result = sorted(result, key=lambda x : x[1], reverse=True)

    return result[:10]

def generateImg(result, path = "./"):
    Company, Price = zip(*result)
    plt.bar(Company, Price)
    plt.xlabel('Company')
    plt.ylabel('Max Price')
    plt.title('top10MostExpensiveYear.png')
    plt.xticks(rotation=45, ha="right")
    plt.savefig(os.path.join(path, 'top10MostExpensiveYear.png'))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit top10MostExpensiveYear.py <dataset dir> <year>")

    result = top10MostExpensiveYear(sys.argv[1], sys.argv[2])
    print(result)

    if len(sys.argv) > 3:
        generateImg(result,sys.argv[3])
    else:
        generateImg(result)
    