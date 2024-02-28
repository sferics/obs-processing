ln -s conclude_obs.py finalize_obs.py
git clone https://github.com/sferics/plbufr/
conda env create -f config/obs_env.yml
cd plbufr && python setup.py install
