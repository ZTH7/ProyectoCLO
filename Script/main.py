
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

        if opcion == "1":
            
        elif opcion == "2":
            year = input("Ingrese el año: ")
            
        elif opcion == "3":
            pais = input("Ingrese el país: ")
            
        elif opcion.lower() == "q":
            print("Saliendo del programa.")
            fin = True
        else:
            print("Opción no válida. Inténtelo de nuevo.")

if __name__ == "__main__":
    main()
