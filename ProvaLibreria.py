import json
from data_filter_lib import DataFilter  


try:
    filtro = DataFilter("config.json")  
except Exception as e:
    print(f"Warning Inizializzazione: {e}")
    exit(1)



try:
    results = filtro.filter_data(
        ["17120003-00-19/001-000339", "sensoreprova", "altrosensoreprova"], 
        "2024-12-03", 
        "2024-12-04", 
        120, 
        ["humidity", "temperature", "pressione", "temp"]
    )

    
    print(json.dumps(results, indent=4))

    '''
    for sensore, result in results.items():
        print(f"Sender: {sensore}")
        print("Dati:")
        print(json.dumps(result["data"], indent=4))
        
        if result["warning"]:
            print("Warning:")
            for campo, messaggio in result["warning"].items():
                print(f"  - {campo}: {messaggio}")

        print("\n")
        print("\n")

    '''

except ValueError as ve:
    print(f"Errore nei parametri: {ve}")
except Exception as e:
    print(f"Errore imprevisto: {e}")


