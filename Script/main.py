import os
from top10MostExpensiveUS import top10MostExpensiveUS
from top10MostExpensiveUSYear import top10MostExpensiveUSYear
from predictionUSYear import predictionUSYear

# TODO: cambiar los prints y completar opciones

def main():
    
    fin = False

    while fin != True:
        print("\nMenú:")
        print("1. Ver 10 acciones más caras históricamente")
        print("2. Ver 10 acciones más caras en un determinado <year>")
        print("3. Ver 10 acciones más caras en un determinado <country> históricamente")
        print("4. Ver 10 acciones más caras en un determinado <country> en un determinado <year>")
        print("5. Ver 5 acciones con mayor crecimiento históricamente")
        print("6. Ver 5 acciones con mayor crecimiento en un determinado <year>")
        print("7. Ver 5 acciones con mayor crecimiento en un determinado <country> históricamente")
        print("8. Ver 5 acciones con mayor crecimiento en un determinado <country> en un determinado <year>")
        print("9. Ver cuál es la probabilidad de que una acción aumente de valor en un determinado <year>")

        opcion = input("\nIngrese el número de la opción que desea: ")
        path = os.getcwd() + "/../Samples"

        if opcion == "1":
            # Unir las 2 listas
            global_tops = top10MostExpensiveUS(path+"/US_data")
            sorted(global_tops, key=lambda x : x[1], reverse=True)
            global_tops = global_tops[:10]
            print(global_tops)

        elif opcion == "2":
            year = input("Ingrese el año: ")
            us_tops = top10MostExpensiveUSYear(path + "/US_data", year)
            print(us_tops)

        elif opcion == "3":
            pais = input("Ingrese el país: ")
        # ...

        elif opcion == "9":
            year = input("Ingrese el año: ")
            prob = predictionUSYear(path+"/US_data", year)
            print(f"La probabilidad de aumento de valor de una acción en el año {year} es de {prob} %")
            
        elif opcion.lower() == "q":
            print("Saliendo del programa.")
            fin = True
        else:
            print("Opción no válida. Inténtelo de nuevo.")

if __name__ == "__main__":
    main()
