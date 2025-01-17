mkdir -p logs  # Create logs directory to store output and error files

while read -r disaster; do
    sbatch --job-name="$disaster" process_disaster.slurm "$disaster"
done < disasters.txt
