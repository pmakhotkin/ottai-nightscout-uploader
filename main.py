from module import *
import schedule
import time

def start_module():


# get last entry date
    # try:
    #     ns_last_date = get_last_entry_date()
    #     if ns_last_date == "":
    #          ns_last_date=0
    # except Exception as error:
    #         print("Error requesting from Nightscout:", error)

# get ottai data
    os.system('cls' if os.name == 'nt' else 'clear')
    try:
        #ottai_data = get_ottai_one_entry()
        ottai_data = get_ottai_array_of_entries()
    except Exception as error:
        print("Error requesting from ottai:", error)
        return
    try:  
        process_json_data(ottai_data)
    except Exception as error:
        print("Error reading direction:", error)

def main():
    start_module()
    schedule.every(1).minutes.do(start_module)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()