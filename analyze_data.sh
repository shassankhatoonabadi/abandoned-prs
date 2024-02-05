#!/bin/bash

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" &&
    python3 preprocess_data.py -n &&
    python3 process_data.py -n &&
    python3 postprocess_data.py -n &&
    python3 analyze_inactivity.py -n &&
    python3 prelabel_data.py -n &&
    python3 label_data.py -n &&
    python3 calculate_agreement.py -n &&
    python3 extract_developers.py -n &&
    python3 analyze_survey.py -n &&
    python3 measure_features.py -n &&
    python3 build_deeplearning.py -n &&
    echo "Finished analyzing data"
