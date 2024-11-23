data = {"type": "user", "info": {"name": "Alice", "age": 30}}  

match data:  
    case {"type": "user", "info": {"name": str(name), "age": int(age)}}:  
        print(f"User: {name}, Age: {age}")