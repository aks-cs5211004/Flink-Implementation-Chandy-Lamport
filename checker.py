import json

# Load the reducer data
r1 = open("checkpoints/Reducer_0_0.txt").read()
r2 = open("checkpoints/Reducer_1_0.txt").read()
r1 = json.loads(r1)
r2 = json.loads(r2)

# Merge both reducers' data
r = r1 | r2

# Load the correct data
correct = json.loads(open("seq.txt").read())

# Print all counts side by side
print(f"{'Key':<20} {'Reducer Count':<15} {'Correct Count'}")
for key in sorted(set(r.keys()) | set(correct.keys())):
    r_count = r.get(key, 0)
    correct_count = correct.get(key, 0)
    print(f"{key:<20} {r_count:<15} {correct_count}")

# Check if they match
if r == correct:
    print("CORRECT")
else:
    print("INCORRECT")
