-- Журналы мониторинга хранятся 90 дней: этого хватает для дашборда здоровья пайплайна
DELETE FROM ops_task_runs WHERE finished_at < now() - interval '90 days';
DELETE FROM ops_dq_results WHERE checked_at < now() - interval '90 days';
