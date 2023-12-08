import os
from top10MostExpensive import top10MostExpensive
from top10MostExpensiveYear import top10MostExpensiveYear
from top10MostExpensiveUS import top10MostExpensiveUS
from top10MostExpensiveUSYear import top10MostExpensiveUSYear
from top10MostExpensiveChina import top10MostExpensiveChina
from top10MostExpensiveChinaYear import top10MostExpensiveChinaYear
from top10MostExpensiveIndia import top10MostExpensiveIndia
from top10MostExpensiveIndiaYear import top10MostExpensiveIndiaYear
from predictionUSYear import predictionUSYear
from top5HighestGrowth import top5HighestGrowth
from top5HighestGrowthYear import top5HighestGrowthYear
from top5HighestGrowthUS import top5HighestGrowthUS
from top5HighestGrowthUSYear import top5HighestGrowthUSYear
from top5HighestGrowthIndia import top5HighestGrowthIndia
from top5HighestGrowthIndiaYear import top5HighestGrowthIndiaYear
from top5HighestGrowthChina import top5HighestGrowthChina
from top5HighestGrowthChinaYear import top5HighestGrowthChinaYear


def main():
    
    end = False

    while not end:
        print("\nMenu:")
        print("1. View the 10 historically most expensive stocks")
        print("2. View the 10 most expensive stocks in a specific <year>")
        print("3. View the 10 most expensive stocks in a specific <country> historically")
        print("4. View the 10 most expensive stocks in a specific <country> in a specific <year>")
        print("5. View the 5 stocks with the highest historical growth")
        print("6. View the 5 stocks with the highest growth in a specific <year>")
        print("7. View the 5 stocks with the highest growth in a specific <country> historically")
        print("8. View the 5 stocks with the highest growth in a specific <country> in a specific <year>")
        print("9. View the probability of a stock increasing in value in a specific <year>")
        print("q : If you want to exit the program")

        option = input("\nEnter the number of the option you want: ")
        path = os.getcwd() + "/../Samples"

        if option == "1":
            global_tops = top10MostExpensive(path)
            print(global_tops)

        elif option == "2":
            try:
                year = int(input("Enter the year: "))
            except ValueError:
                print("Not valid year")
            tops = top10MostExpensiveYear(path, year)
            print(tops)

        elif option == "3":
            country = input("Enter the country (US, China, India): ")
            if country == "US":
                print(top10MostExpensiveUS(path+r"/US_data"))
            elif country == "China":
                print(top10MostExpensiveChina(path+r"/China_data"))
            elif country == "India":
                print(top10MostExpensiveIndia(path+r"/India_data"))
            else:
                print("Not valid country")

        elif option == "4":
            country = input("Enter the country (US, China, India): ")
            try:
                year = int(input("Enter the year: "))
            except ValueError:
                print("Not valid year")
            if country == "US":
                print(top10MostExpensiveUSYear(path+r"/US_data", year))
            elif country == "China":
                print(top10MostExpensiveChinaYear(path+r"/China_data", year))
            elif country == "India":
                print(top10MostExpensiveIndiaYear(path+r"/India_data", year))
            else:
                print("Not valid country")
        
        elif option == "5":
            print(top5HighestGrowth(path))
        
        elif option == "6":
            try:
                year = int(input("Enter the year: "))
            except ValueError:
                print("Not valid year")
            print(top5HighestGrowthYear(path, year))

        elif option == "7":
            country = input("Enter the country (US, China, India): ")
            if country == "US":
                print(top5HighestGrowthUS(path+r"/US_data"))
            elif country == "China":
                print(top5HighestGrowthChina(path+r"/China_data"))
            elif country == "India":
                print(top5HighestGrowthIndia(path+r"/India_data"))
            else:
                print("Not valid country")
            
        elif option == "8":
            country = input("Enter the country (US, China, India): ")
            try:
                year = int(input("Enter the year: "))
            except ValueError:
                print("Not valid year")
            if country == "US":
                print(top5HighestGrowthUSYear(path+r"/US_data", year))
            elif country == "China":
                print(top5HighestGrowthChinaYear(path+r"/China_data", year))
            elif country == "India":
                print(top5HighestGrowthIndiaYear(path+r"/India_data", year))
            else:
                print("Not valid country")

        elif option == "9":
            try:
                year = int(input("Enter the year: "))
            except ValueError:
                print("Not valid year")
            prob = predictionUSYear(path+r"/US_data", year)
            print(f"The probability of a stock increasing in value in the year {year} is {round(prob, 2)}%")
            
        elif option.lower() == "q":
            print("Exiting the program.")
            end = True
        else:
            print("Invalid option. Please try again.")

if __name__ == "__main__":
    main()
