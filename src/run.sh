# echo "-2 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q013a-6B.json
# echo "-1 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q013b-6B.json
# echo "0 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q018-6B.json
# echo "1 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q019-6B.json
# echo "2 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q040-6B.json
# echo "3 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q072-6B.json
# echo "4 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q085a-6B.json
# echo "5 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q085b-6B.json
# echo "6 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q099-6B.json
# echo "7 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q100-6B.json
# echo "7 true ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q102-6B.json

# echo "1 precompute ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 precompute_truecards1d.py ../input_configs/dsb_sf2_precompute_config_Q040-6B.json

# echo "4/11 1 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q013a-6B_trial2_jgmp.json
# echo "4/11 2 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q013a-6B_95th_jgmp.json
# echo "4/11 3 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q013a-6B_rt3_jgmp.json
# echo "4/11 4 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q013a-6B_jgmp.json

# echo "4/11 5 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q013b-6B_trial2_jgmp.json
# echo "4/11 6 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q013b-6B_95th_jgmp.json
# echo "4/11 7 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q013b-6B_rt3_jgmp.json
# echo "4/11 8 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q013b-6B_jgmp.json

# echo "4/11 9 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q100-6B_trial2_jgmp.json
# echo "4/11 10 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q100-6B_95th_jgmp.json
# echo "4/11 11 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q100-6B_rt3_jgmp.json
# echo "4/11 12 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q100-6B_jgmp.json

# echo "4/12 Q040-6B 0 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_Q040-6B.json
# echo "4/12  Q040-6B 1 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q040-6B_jgmp.json
# echo "4/12  Q040-6B 2 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_trial2_jgmp.json
# echo "4/12 Q040-6B 3 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q040-6B_rt6_jgmp.json
# echo "4/12 Q040-6B 4 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q040-6B_jgmp.json

# echo "4/12 1 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q018-6B_trial2_jgmp.json
# echo "4/12 2 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q018-6B_95th_jgmp.json
# echo "4/12 3 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q018-6B_rt3_jgmp.json
# echo "4/12 4 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q018-6B_jgmp.json

# echo "4/12 6 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q019-6B_95th_jgmp.json
# echo "4/12 7 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q019-6B_rt3_jgmp.json
# echo "4/12 8 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q019-6B_jgmp.json

# echo "4/12 9 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_correctjoin2_trial2_jgmp.json
# echo "4/12 10 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q040-6B_95th_jgmp.json
# echo "4/12 11 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q040-6B_rt3_jgmp.json
# echo "4/12 12 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q040-6B_jgmp.json

# echo "4/12 13 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q072-6B_trial2_jgmp.json
# echo "4/12 14 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q072-6B_95th_jgmp.json
# echo "4/12 15 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q072-6B_rt3_jgmp.json
# echo "4/12 16 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q072-6B_jgmp.json

# echo "4/12 18 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q085a-6B_95th_jgmp.json
# echo "4/12 19 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q085a-6B_rt3_jgmp.json
# echo "4/12 20 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q085a-6B_jgmp.json

# echo "4/12 22 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q085b-6B_95th_jgmp.json
# echo "4/12 23 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q085b-6B_rt3_jgmp.json
# echo "4/12 24 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q085b-6B_jgmp.json

# echo "4/12 25 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q099-6B_correctjoin2_trial2_jgmp.json
# echo "4/12 26 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q099-6B_95th_jgmp.json
# echo "4/12 27 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q099-6B_rt3_jgmp.json
# echo "4/12 28 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q099-6B_jgmp.json

# echo "4/12 30 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q102-6B_95th_jgmp.json
# echo "4/12 31 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q102-6B_rt3_jgmp.json
# echo "4/12 32 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q102-6B_jgmp.json


# echo "4 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q013b-6B_jgmp.json
# echo "5 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q013b-6B_correctjoin2_trial2_jgmp.json
# echo "6 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q013b-6B_rt6_jgmp.json
# echo "7 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q013b-6B_jgmp.json

# echo "8 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q100-6B_jgmp.json
# echo "9 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q100-6B_correctjoin2_trial2_jgmp.json
# echo "10 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q100-6B_rt6_jgmp.json
# echo "11 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q100-6B_jgmp.json

# # echo "12 ---------"
# # dropdb -p 5555 watcherdb
# # createdb -p 5555 -U yl762 watcherdb
# # python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q018-6B_jgmp.json
# # echo "13 ---------"
# # dropdb -p 5555 watcherdb
# # createdb -p 5555 -U yl762 watcherdb
# # python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q018-6B_correctjoin2_trial2_jgmp.json
# echo "14 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q018-6B_rt6_jgmp.json
# echo "15 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q018-6B_jgmp.json

# echo "16 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q040-6B_jgmp.json
# # echo "17 ---------"
# # dropdb -p 5555 watcherdb
# # createdb -p 5555 -U yl762 watcherdb
# # python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_trial2_jgmp.json
# echo "18 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q040-6B_rt6_jgmp.json
# echo "19 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q040-6B_jgmp.json

# echo "20 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q072-6B_jgmp.json
# echo "21 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q072-6B_correctjoin2_trial2_jgmp.json
# echo "22 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q072-6B_rt6_jgmp.json
# echo "23 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q072-6B_jgmp.json

# echo "24 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q019-6B_jgmp.json
# # echo "25 ---------"
# # dropdb -p 5555 watcherdb
# # createdb -p 5555 -U yl762 watcherdb
# # python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q019-6B_trial2_jgmp.json
# echo "26 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q019-6B_rt6_jgmp.json
# echo "27 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q019-6B_jgmp.json

# echo "28 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q099-6B_jgmp.json
# echo "29 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q099-6B_trial2_jgmp.json
# echo "30 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q099-6B_rt6_jgmp.json
# echo "31 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q099-6B_jgmp.json

# echo "1 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q085a-6B_jgmp.json
# echo "2 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q085a-6B_trial2_jgmp.json
# echo "3 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q085a-6B_rt6_jgmp.json
# echo "4 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q085a-6B_jgmp.json

# echo "5 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q085b-6B_jgmp.json
# echo "6 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q085b-6B_trial2_jgmp.json
# echo "7 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q085b-6B_rt6_jgmp.json
# echo "8 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q085b-6B_jgmp.json

# echo "9 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q101-6B_jgmp.json
# echo "10 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q101-6B_trial2_jgmp.json
# echo "11 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q101-6B_rt6_jgmp.json
# echo "12 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q101-6B_jgmp.json

# echo "13 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_Q102-6B_jgmp.json
# echo "14 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q102-6B_trial2_jgmp.json
# echo "15 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_Q102-6B_rt6_jgmp.json
# echo "16 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_Q102-6B_jgmp.json



# echo "4/17  Q040-6B 1 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_correctjoin2_trial1_2ms.json
# echo "4/17  Q040-6B 2 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_correctjoin2_trial2_2ms_jgmp.json
# echo "4/17  Q040-6B 3 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_trial1_2ms.json
# echo "4/17  Q040-6B 4 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_trial2_2ms_jgmp.json
# echo "4/17  Q040-6B 5 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_2ms.json
# echo "4/17  Q040-6B 6 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_Q040-6B_correctjoin2_2ms.json


# echo "9/22 Q013a-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q013a-6B.json
# echo "9/22 Q013b-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q013b-6B.json
# echo "9/22 Q018-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q018-6B.json
# echo "9/22 Q019-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q019-6B.json
# echo "9/22 Q040-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q040-6B.json
# echo "9/22 Q072-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q072-6B.json
# echo "9/22 Q085a-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q085a-6B.json
# echo "9/22 Q085b-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q085b-6B.json
# echo "9/22 Q099-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q099-6B.json
# echo "9/22 Q100-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q100-6B.json
# echo "9/22 Q101-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q101-6B.json
# echo "9/22 Q102-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q102-6B.json

# echo "9/23 Q013a-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q013a-6B_jgmp.json
# echo "9/23 Q013b-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q013b-6B_jgmp.json
# echo "9/23 Q018-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q018-6B_jgmp.json
# echo "9/23 Q019-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q019-6B_jgmp.json
# echo "9/23 Q040-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q040-6B_jgmp.json
# echo "9/23 Q072-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q072-6B_jgmp.json
# echo "9/23 Q085a-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q085a-6B_jgmp.json
# echo "9/23 Q085b-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q085b-6B_jgmp.json
# echo "9/23 Q099-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q099-6B_jgmp.json
# echo "9/23 Q100-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q100-6B_jgmp.json
# echo "9/23 Q102-6B ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q102-6B_jgmp.json

# echo "9/29 PQ001 execute training set ------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 execute_trainingset1d.py ../input_configs/dsb_sf2_trainingset_config_PQ001.json
# echo "9/29 PQ001 precompute ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 precompute_truecards1d.py ../input_configs/dsb_sf2_precompute_config_PQ001.json
# echo "9/29 PQ001 true card -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main1d.py ../input_configs/dsb_sf2_true-card_config_PQ001.json
# echo "9/29  PQ001 nothing -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main1d.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_PQ001.json
# echo "9/29 PQ001 baseline 1 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_PQ001.json
# echo "9/29 PQ001 baseline 3 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_PQ001_rt3.json
# echo "9/29 PQ001 baseline 6 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 baseline1d.py ../input_configs/dsb_sf2_baseline_config_PQ001_rt6.json
# echo "9/29 PQ001 watcher1d 90th ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_PQ001.json
# echo "9/29 PQ001 watcher1d 95th ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 main1d.py ../input_configs/dsb_sf2_config_PQ001_95th.json
# echo "9/29 PQ001 leo ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_PQ001.json
# echo "9/30 PQ001 twotier ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_PQ001.json
# echo "9/30 PQ001 twotier trial 1 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 twotier_main1d.py ../input_configs/dsb_sf2_twotier_config_PQ001_trial1.json

# echo "10/06 Q013a ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q013a.json
# echo "10/06 Q013b ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q013b.json
# echo "10/06 Q018 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q018.json
# echo "10/06 Q019 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q019.json
# echo "10/06 Q040 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q040.json
# echo "10/06 Q072 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q072.json
# echo "10/06 Q085a ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q085a.json
# echo "10/06 Q085b ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q085b.json
# echo "10/06 Q099 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q099.json
# echo "10/06 Q100 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q100.json
# echo "10/06 Q101 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q101.json
# echo "10/06 Q102 ---------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 leo_main1d.py ../input_configs/dsb_sf2_leo_config_Q102.json




# echo "1/22  Q100 0 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 precompute_truecards_multid.py ../input_configs/dsb_sf2_precompute_config_THJQ100.json
# echo "1/22  Q100 1 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 execute_trainingset_multid.py ../input_configs/dsb_sf2_trainingset_config_THJQ100.json
# echo "1/22  Q100 2 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 execute_subquery_in_trainingset_multid.py ../input_configs/dsb_sf2_subqueries_in_trainingset_config_THJQ100.json
# echo "1/22  Q100 3 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 truecard_main_multid.py ../input_configs/dsb_sf2_true-card_config_THJQ100.json
# echo "1/22  Q100 4 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 base_main_multid.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_THJQ100.json
# echo "1/22  Q100 5 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_routinely_config_THJQ100.json
# echo "1/22  Q100 6 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_routinely_config_THJQ100_rt3.json

# echo "1/22  Q100 7 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_twotier_config_THJQ100.json
# echo "1/22  Q100 8 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_twotier_config_THJQ100_trial1.json
# echo "1/22  Q100 9 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_twotier_config_THJQ100_correctjoin3.json
# echo "1/22  Q100 10 -------"
# dropdb -p 5555 watcherdb
# createdb -p 5555 -U yl762 watcherdb
# python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_twotier_config_THJQ100_correctjoin3_trial1.json


echo "1/27  Q100 0 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 precompute_truecards_multid.py ../input_configs/dsb_sf2_precompute_config_FJQ100.json
echo "1/27  Q100 1 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 execute_trainingset_multid.py ../input_configs/dsb_sf2_trainingset_config_FJQ100.json
echo "1/27  Q100 2 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 execute_subquery_in_trainingset_multid.py ../input_configs/dsb_sf2_subqueries_in_trainingset_config_FJQ100.json
echo "1/27  Q100 3 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 truecard_main_multid.py ../input_configs/dsb_sf2_true-card_config_FJQ100.json
echo "1/27  Q100 4 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 base_main_multid.py ../input_configs/dsb_sf2_no-retrain_no-cache_config_FJQ100.json
echo "1/27  Q100 5 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_routinely_config_FJQ100.json

echo "1/27  Q100 7 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_twotier_config_FJQ100.json
echo "1/27  Q100 8 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_twotier_config_FJQ100_trial1.json
echo "1/27  Q100 9 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_twotier_config_FJQ100_correctjoin4.json
echo "1/27  Q100 10 -------"
dropdb -p 5555 watcherdb
createdb -p 5555 -U yl762 watcherdb
python3 collaborate_watchers_multid.py ../input_configs/dsb_sf2_collaborate_twotier_config_FJQ100_correctjoin4_trial1.json