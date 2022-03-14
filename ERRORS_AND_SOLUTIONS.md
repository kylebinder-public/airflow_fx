# airflow_fx
Repo of FX pipeline orchestration.

Implements Section 3 of https://www.udemy.com/course/the-ultimate-hands-on-course-to-master-apache-airflow/.

# COMMON ERRORS & SOLUTIONS:

(1) If you see [SSL: CERTIFICATE_VERIFY_FAILED]:

If you're only concerned with getting the pipeline to run, generally all you need to do is append the argument "--no-check-certificate" to no longer see the above message.

For me, this needed to happen not only in specific python calls, but also in the initial builds of Docker containers.


(2) .BAT vs .SH

In order to run from a Windows terminal, .SH files can be renamed with the .BAT extension.
