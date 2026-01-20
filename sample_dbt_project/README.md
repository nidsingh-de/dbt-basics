
### Using the sample project

Try running the following commands:

- Cleaning project : ```dbt clean```
- Installing dependencies : ```dbt deps```

- Seed CSV: ```dbt seed --profiles-dir profile --target reference```
- Run models: ```dbt run --profiles-dir profile```

- Generate control messages: ```dbt run-operation run_validation --profiles-dir profile --vars "<content of rules.yaml file>"```

- Schema test: ```dbt test --profiles-dir profile --select tag:schema-test```
- Unit test: ```dbt test --profiles-dir profile --select tag:unit-test```

- Generate dbt docs: ```dbt docs generate --profiles-dir profile```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
