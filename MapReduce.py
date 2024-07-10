import multiprocessing as mp
from functools import reduce

# Sample large dataset of students' grades
students = [
    {'name': f'Student {i}', 'grades': {'math': i % 100, 'science': (i * 2) % 100, 'english': (i * 3) % 100}}
    for i in range(1, 10001)
]

# Threshold for filtering students
threshold = 75

# filter by threshold avg
def filter_by_avg(student):
    return list(filter(lambda x: sum(x['grades'].values()) / len(x['grades']) > threshold, student))

# Function to calculate the average grade for each student
def calculate_average(students_chunk):
    return list(map(lambda student: {'name': student['name'], 'average': sum(student['grades'].values()) / len(student['grades'])}, students_chunk))



# Function to find the top N students in a chunk
def find_top_students(students_chunk, n=5):
    return sorted(students_chunk, key=lambda x: x['average'], reverse=True)[:n]


# Function to combine top students lists
def combine_top_students(top_students_lists, n=5):
    combined = [student for sublist in top_students_lists for student in sublist]
    combined_sorted = sorted(combined, key=lambda x: x['average'], reverse=True)
    return combined_sorted[:n]

if __name__ == "__main__":

    # Number of processes to use
    num_processes = mp.cpu_count()
    print(f'Number of processes: {num_processes}')

    # Step 1: Split data into chunks for each process
    chunk_size = len(students) // num_processes

    print(f'Chunk size: {chunk_size}')

    chunks = [students[i:i+chunk_size] for i in range(0, len(students), chunk_size)]

    with mp.Pool(num_processes) as pool:
        filtered_students = pool.map(filter_by_avg, chunks)
        average_chunks = pool.map(calculate_average, filtered_students)

    # Step 3: Find top students in parallel
    with mp.Pool(processes=num_processes) as pool:
        top_students_chunks = pool.starmap(find_top_students, [(chunk, 5) for chunk in average_chunks])


    top_students = combine_top_students(top_students_chunks)

    print("Top 5 students with the highest average grades:")
    for student in top_students:
        print(f"{student['name']} with an average grade of {student['average']:.2f}")



