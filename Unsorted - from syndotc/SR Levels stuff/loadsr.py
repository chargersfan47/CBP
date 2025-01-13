import pyautogui
import time
import pandas as pd

data_file = 'LoadSR\support_and_resistance_levels.csv'  # Replace with your CSV file path
data = pd.read_csv(data_file)

def add_horizontal_line(timeframe, level_type, level, Timestamp, delay=1):
    pyautogui.click(3094, 1790)  # Focus on the chart (adjust coordinates as needed)

    # Simulate the Option + H command to add a horizontal line
    pyautogui.keyDown('alt')  # Press and hold the Option key
    pyautogui.press('h')         # Press the H key
    pyautogui.keyUp('alt')    # Release the Option key
    time.sleep(delay)

    pyautogui.click(3094, 1790)
    pyautogui.click(3094, 1790)
    time.sleep(delay)
    
    # Click the text button at its position
    #pyautogui.click(1778, 892)  # Adjust coordinates if needed for the text button
    #time.sleep(delay)
    
    # Type the timeframe and level type
    label_text = f"{timeframe} {level_type} {Timestamp}"
    pyautogui.typewrite(label_text)
    time.sleep(delay)
    
    # Click the coordinates button at its position
    pyautogui.click(1901, 885)  # Adjust coordinates if needed for coordinance button
    time.sleep(delay)
    
    # Type the level
    pyautogui.typewrite(str(level))
    time.sleep(delay)
    
    # Press Enter to confirm
    pyautogui.press('enter')
    time.sleep(delay)
    
    # Escape to close the dialog
    pyautogui.press('esc', presses=2)
    time.sleep(delay)

def main():
    # Iterate over each row in the DataFrame
    for index, row in data.iterrows():
        timeframe = row['Timeframe']
        level_type = row['Level Type']
        level = row['Level']
        Timestamp = row['Found Timestamp']
        print(f"Adding horizontal line for {timeframe} {level_type} at level {level}...")
        add_horizontal_line(timeframe, level_type, level, Timestamp)
        time.sleep(1)  # Add a short delay between entries

if __name__ == '__main__':
    time.sleep(5)  # Delay to focus Chrome
    main()