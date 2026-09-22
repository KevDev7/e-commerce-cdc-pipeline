# Project guidance

This is a student data engineering portfolio project. Optimize for learning, clarity, and a complete working example that the student can explain in an interview.

- Prefer the simplest approach that demonstrates the intended skill. When several approaches work, favor the one that is easiest to understand, run, debug, and explain.
- Keep the initial scope to Olist customers, orders, order items, and payments.
- The final deliverable ends at populated, tested Redshift marts. Dashboards, BI applications, and data visualizations are out of scope.
- Demonstrate real PostgreSQL log-based CDC using a clearly labeled simulated business workload. Be accurate about what is simulated, implemented, and tested.
- Use the agreed warehouse layers: raw, staging, intermediate, and marts. Put preparation for marts in intermediate; add models only when they do useful work.
- Preserve essential correctness: ordered change application, delete handling, safe retries, replay, and source-to-target reconciliation. Use focused checks appropriate to the implementation.
- Add services, frameworks, abstractions, and production features only when they support a concrete project requirement or learning objective. Avoid enterprise-scale infrastructure for hypothetical future needs.
- Keep AWS resources and credentials separate from other projects. Use the synthea-cdc AWS profile explicitly. Establish a spending limit before provisioning paid infrastructure.
- Commit and push small, coherent, verified milestones with descriptive messages. Do not postpone all pushes until the entire pipeline is finished. Never commit credentials or downloaded datasets.
- Keep datasets in S3 between demonstrations; delete paid compute afterward. Do not keep a full source or warehouse database on the user's Mac. Temporary seed files, small fixtures and sanitized reports are acceptable.
