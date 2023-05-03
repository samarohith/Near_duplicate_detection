s=("aids" "muta" "tox" "mcf" "molt")
for e in ${s[@]}; do 
    ./filter $e"_veo.txt" 5 96 stat;
done;

