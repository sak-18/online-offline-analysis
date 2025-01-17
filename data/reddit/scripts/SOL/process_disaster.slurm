#!/bin/bash
#SBATCH -N 1                    # Number of nodes
#SBATCH -c 24                    # Number of CPU cores
#SBATCH --mem=256G               # Memory allocation (adjust as needed)
#SBATCH -t 0-12:00:00           # Time limit (12 hours)
#SBATCH -p general              # Partition name
#SBATCH -q public       # QOS
#SBATCH -o logs/%x_%j.out       # STDOUT log file (%x=job name, %j=job ID)
#SBATCH -e logs/%x_%j.err       # STDERR log file
#SBATCH --mail-type=END,FAIL    # Email notifications for job completion/failure
#SBATCH --mail-user=svishnu6@gmail.com  # Your email for notifications

# Load necessary modules and activate environment
module load mamba

pip install -r ../../../requirements.txt

# Get the disaster name from the command line
DISASTER_NAME=$1

# Print start message
echo "Processing disaster: $DISASTER_NAME"
echo "Job started on $(hostname) at $(date)"

# Run the Python script with the provided disaster name
python create_subreddit_dataset_sol.py --disaster_name "$DISASTER_NAME"

# Print completion message
echo "Job completed for disaster: $DISASTER_NAME at $(date)"
