To test the condor setup, after starting the docker compose, shell into the submit node:

```
docker compose exec -it -u submituser htcondor-mini bash
```

Then run

```
condor_submit /opt/submit_test_script
watch condor_q
```