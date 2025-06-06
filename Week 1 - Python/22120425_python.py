import pandas as pd

def sum_of_odds():
    total = 0
    for i in range(1, 101):
        if i % 2 != 0:
            total += i
    return total

def find_numbers():
    for i in range(2000, 3201):
        if i % 7 == 0 and i % 5 != 0:
            print(i, end=" ")

def reverse_string():
    s = input("Enter a string: ")
    return s[::-1]

def factorial():
    while True:
        try:
            n = int(input("Enter a positive integer n: "))
            if n < 0:
                raise ValueError("Invalid number")
            break
        except ValueError as e:
            print(e)
            return None
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result

def fizz_buzz():
    numbers = list(range(1, 51))
    for number in numbers:
        if number % 3 == 0 and number % 5 == 0:
            print("FizzBuzz", end="")
        elif number % 3 == 0:
            print("Fizz", end="")
        elif number % 5 == 0:
            print("Buzz", end="")

def is_palindrome():
    s = input("Enter a string: ")
    s = s.lower()
    if s == s[::-1]:
        print("The string is a palindrome")
    else:
        print("The string is not a palindrome")

def find_max():
    while True:
        try:
            numbers = list(map(int, input("Enter a list of integers separated by spaces: ").split()))
            if not numbers:
                raise ValueError("Empty list! Please enter at least one number.")
            return max(numbers)
        except ValueError as e:
            print(e)

def remove_duplicates():
    while True:
        try:
            numbers = list(map(int, input("Enter a list of integers separated by spaces: ").split()))
            if not numbers:
                raise ValueError("Empty list! Please enter at least one number.")
            break
        except ValueError as e:
            print(f"Error: {e}. Please try again!")

    unique_numbers = list(set(numbers))
    print("List after removing duplicates:")
    for number in unique_numbers:
        print(number, end=" ")

def is_prime():
    while True:
        try:
            n = int(input("Enter a positive integer n: "))
            if n < 1:
                raise ValueError("Invalid number")
            break
        except ValueError as e:
            print(e)
            return None

    for i in range(2, n // 2 + 1):
        if n % i == 0:
            print(f"{n} is not a prime number")
            return
    print(f"{n} is a prime number")

def sum_list_or_tuple():
    while True:
        try:
            user_input = input("Enter a list or tuple of numbers separated by spaces or commas: ")
            user_input = user_input.replace(" ", ",")
            try:
                numbers = [float(x) for x in user_input.split(',')]
            except ValueError:
                raise ValueError("Input must be numbers!")
            return sum(numbers)
        except (ValueError, SyntaxError) as e:
            print(e)

def read_and_display_csv():
    file_name = input("Enter the CSV file name: ")
    try:
        df = pd.read_csv(file_name)
        print("CSV file content:")
        print(df)
    except FileNotFoundError:
        print(f"File {file_name} does not exist. Please check the file name.")
    except Exception as e:
        print(e)

def sum_or_average_csv_column():
    file_name = input("Enter the CSV file name: ")
    try:
        df = pd.read_csv(file_name)
        column = input("Enter the column name to calculate sum or average: ")
        if column not in df.columns:
            print(f"Column '{column}' does not exist in the file.")
            return

        df[column] = pd.to_numeric(df[column], errors='coerce')

        if df[column].isna().any():
            print("The column contains non-numeric values. Please try again!")
            return

        choice = input("Do you want to calculate:\n1. Sum\n2. Average\nYour choice: ").strip()

        if choice == "1":
            result = df[column].sum()
        elif choice == "2":
            result = df[column].mean()
        else:
            print("Invalid choice.")
            return

        print(f"Result: {result}")
    except FileNotFoundError:
        print(f"File {file_name} does not exist. Please check the file name.")
    except Exception as e:
        print(e)

def add_data_to_csv():
    file_name = input("Enter the CSV file name: ")

    try:
        df = pd.read_csv(file_name)

        print("Existing columns in the file:")
        print(df.columns.tolist())

        new_row = {}
        for col in df.columns:
            new_row[col] = input(f"Enter value for column '{col}': ")
        new_df = pd.DataFrame([new_row])

        df = pd.concat([df, new_df], ignore_index=True)

        df.to_csv(file_name, index=False)
        print("New row has been added to the CSV file.")
    except FileNotFoundError:
        print(f"File {file_name} does not exist. Please check the file name.")
    except Exception as e:
        print(e)

def menu():
    print("Student ID: 22120425")
    print("Full name: Nguyễn Thị Uyển Nhi")
    print("\nThis program includes basic Python exercises.")

    menu_options = [
        ("1", "Sum of odd numbers from 1 to 100"),
        ("2", "Numbers divisible by 7 but not by 5 from 2000 to 3200"),
        ("3", "Reverse a string"),
        ("4", "Calculate factorial"),
        ("5", "FizzBuzz"),
        ("6", "Check if a string is a palindrome"),
        ("7", "Find the largest number in a list"),
        ("8", "Remove duplicates from a list"),
        ("9", "Check if a number is prime"),
        ("10", "Sum of list or tuple of numbers"),
        ("11", "Read and display CSV file"),
        ("12", "Sum or average a column in CSV file"),
        ("13", "Add data to a CSV file"),
        ("0", "Exit")
    ]

    for option in menu_options:
        print(f"{option[0]}. {option[1]}")

    while True:
        choice = input("\nEnter your choice: ")

        if choice == "1":
            print("Sum of odd numbers from 1 to 100 is:", sum_of_odds())
        elif choice == "2":
            print("Numbers divisible by 7 but not by 5 between 2000 and 3200:")
            find_numbers()
        elif choice == "3":
            print("Reversed string:", reverse_string())
        elif choice == "4":
            print("Factorial of n is:", factorial())
        elif choice == "5":
            fizz_buzz()
        elif choice == "6":
            is_palindrome()
        elif choice == "7":
            print("Largest number in the list is:", find_max())
        elif choice == "8":
            remove_duplicates()
        elif choice == "9":
            is_prime()
        elif choice == "10":
            print("Sum of list or tuple is:", sum_list_or_tuple())
        elif choice == "11":
            read_and_display_csv()
        elif choice == "12":
            sum_or_average_csv_column()
        elif choice == "13":
            add_data_to_csv()
        elif choice == "0":
            print("Program ended.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    menu()
