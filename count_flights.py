import os
import tqdm
import json


def count_flights_in_file(file_path):
    flight_count = 0
    chunk_size = 100 * 1024 * 1024  # 100 MB

    def find_complete_json_objects(buffer):
        objects = []
        while True:
            try:
                data, index = json.JSONDecoder().raw_decode(buffer)
                objects.append(data)
                buffer = buffer[index:].lstrip()
            except json.JSONDecodeError:
                break
        return objects, buffer

    with open(file_path, "r") as file:
        buffer = ""
        file_size = os.path.getsize(file_path)
        with tqdm.tqdm(
            total=file_size,
            unit="B",
            unit_scale=True,
            desc=f"Processing {os.path.basename(file_path)}",
        ) as pbar:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                buffer += chunk
                pbar.update(len(chunk))

                objects, buffer = find_complete_json_objects(buffer)
                for data in objects:
                    for item in data.get("timelineObjects", []):
                        if "activitySegment" in item:
                            activity_type = item["activitySegment"].get("activityType")
                            if activity_type == "FLYING":
                                print(
                                    f"File: {file_path}, Found activity type: {activity_type}"
                                )
                                flight_count += 1

    objects, _ = find_complete_json_objects(buffer)
    for data in objects:
        for item in data.get("timelineObjects", []):
            if "activitySegment" in item:
                activity_type = item["activitySegment"].get("activityType")
                if activity_type == "FLYING":
                    print(f"File: {file_path}, Found activity type: {activity_type}")
                    flight_count += 1

    print(f"File: {file_path}, Flight count: {flight_count}")
    return flight_count


def count_flights_in_directory(parent_folder):
    total_flights = 0
    for year in range(2014, 2025):
        year_folder = os.path.join(parent_folder, str(year))
        if os.path.isdir(year_folder):
            for filename in os.listdir(year_folder):
                if filename.endswith(".json"):
                    file_path = os.path.join(year_folder, filename)
                    total_flights += count_flights_in_file(file_path)
    return total_flights


if __name__ == "__main__":
    parent_folder = (
        "Semantic Location History"  # Replace with the actual path to the parent folder
    )
    total_flights = count_flights_in_directory(parent_folder)
    print(f"Total number of flights: {total_flights}")
