# Temporary AWS demonstration: cost plan for approval (T10, blueprint §31 and §33)

**Status: NOT PURSUED (2026-10-12): the owner has no AWS account and chose a free alternative (D-066). Kept as the costed option if a real cloud run is wanted later. Originally: PROPOSED. No AWS resource exists and none will be created until the owner approves this plan in
writing and supplies an account.** I (Claude) cannot create resources: I have no AWS credentials and will
not be given any. The owner runs each step; I supply the infrastructure definition, commands and checks.

## What it proves

Blueprint A12: the same fixture, run on a cloud host from a manifest in S3, gives the same business checksum
as the local run; one interrupted-load scenario recovers; one database restore drill passes.

## Proposed configuration (eu-west-1, one region throughout)

| Item | Choice | Unit price (checked 2026-10-12) | Planned use |
|---|---|---|---|
| Compute | one EC2 instance, size chosen after measuring container memory locally. Candidate `t4g.medium` (2 vCPU, 4 GiB, ARM) at $0.0368/h; if the measured stack needs more, `t4g.large` (8 GiB) at roughly twice that | [aws-pricing.com t4g.medium](https://aws-pricing.com/t4g.medium.html); larger size to be re-checked | 4 h |
| Disk | one encrypted gp3 volume, 30 GB | about $0.09 per GB-month (approximate; re-check at deployment) | 1 day |
| Public IPv4 | one address, zero inbound rules | $0.005/h, [AWS VPC pricing](https://aws.amazon.com/vpc/pricing/) | 4 h |
| Object store | one private S3 bucket, public access blocked, < 1 GB | cents | 1 day |
| Access | Systems Manager Session Manager only; no public Airflow, PostgreSQL or dashboard port | no extra charge | |
| Identity | instance role limited to the bucket's prefixes plus Session Manager | none | |

## Estimate

Compute 4 h × $0.0368 to $0.074 = **$0.15 to $0.30**; disk about **$0.10**; IPv4 **$0.02**; S3 and requests
**< $0.05**; data transfer in and out of the instance for images and packages is small. **Expected total
under $1; proposed hard ceiling $5**, which also covers one rerun after a mistake. These are estimates from
list prices, not a quote; actual prices and the final instance size are recorded in the evidence.

## Controls

1. **Budget alert.** A monthly budget of $5 with e-mail alerts at 50 % and 100 %. An alert is a notification,
   not a spending cap.
2. **Lifetime.** Created and destroyed in one working session. Nothing is left running overnight.
3. **Teardown checklist** (written before creation, executed by the owner): export evidence and a verified
   dump to the bucket, stop and terminate the instance, delete the volume and any snapshots, release the IP,
   empty and delete the bucket, then confirm in Cost Explorer and the console that no resource of the project
   remains.
4. **No secrets in git.** The infrastructure definition takes no credentials; database passwords are generated
   on the host and the Airflow keys are kept on the encrypted volume.
5. **Local first.** Nothing is deployed until the local benchmark, the local backup/restore drill and the
   measured container memory exist. The instance size follows from those numbers.

## What I need from the owner to proceed

- A yes or no to this plan, and any change to the ceiling or lifetime.
- An AWS account whose root user has MFA, and an IAM identity (not the root user) with rights to create the
  listed resources. Do not paste keys into the chat.
- The e-mail address for budget alerts.

If you prefer not to use AWS at all, the project is complete without it except for acceptance A12, which
would stay PENDING and be reported as such. A cheaper partial alternative is a restore drill on a second local
PostgreSQL container, which does not prove cloud parity.
