data = {"type": "user", "info": {"name": "Alice", "age": 30}}  

match data:  
    case {"type": "user", "info": {"name": str(name), "age": int(age)}}:  
        print(f"User: {name}, Age: {age}")
        
number = 0

def loop_test():
    for number in range(10):
        if number == 1:
            break    # break here

    return number

print(loop_test())