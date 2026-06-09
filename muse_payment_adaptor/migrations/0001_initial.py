import uuid

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='MuseTransactionLog',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('uuid', models.UUIDField(default=uuid.uuid4, editable=False, unique=True)),
                ('transaction_type', models.CharField(
                    choices=[('PAYMENT', 'Payment'), ('RECONCILIATION', 'Reconciliation')],
                    max_length=20,
                )),
                ('status', models.CharField(
                    choices=[
                        ('PENDING', 'Pending'),
                        ('SUCCESS', 'Success'),
                        ('FAILED', 'Failed'),
                        ('RETRYING', 'Retrying'),
                        ('REJECTED', 'Rejected by Gateway'),
                    ],
                    default='PENDING',
                    max_length=20,
                )),
                ('payroll_id', models.IntegerField(blank=True, null=True)),
                ('benefit_code', models.CharField(max_length=255)),
                ('account_number', models.CharField(blank=True, max_length=50, null=True)),
                ('fsp_name', models.CharField(blank=True, max_length=100, null=True)),
                ('fsp_type', models.CharField(blank=True, max_length=20, null=True)),
                ('amount', models.DecimalField(decimal_places=2, max_digits=18, null=True)),
                ('muse_reference', models.CharField(blank=True, max_length=255, null=True)),
                ('http_status_code', models.IntegerField(blank=True, null=True)),
                ('response_body', models.TextField(blank=True, null=True)),
                ('error_message', models.TextField(blank=True, null=True)),
                ('attempt_number', models.PositiveSmallIntegerField(default=1)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'db_table': 'muse_TransactionLog',
                'managed': True,
                'ordering': ['-created_at'],
            },
        ),
        migrations.AddIndex(
            model_name='musetransactionlog',
            index=models.Index(fields=['benefit_code', 'transaction_type'], name='muse_txlog_code_type_idx'),
        ),
        migrations.AddIndex(
            model_name='musetransactionlog',
            index=models.Index(fields=['payroll_id', 'status'], name='muse_txlog_payroll_status_idx'),
        ),
        migrations.AddIndex(
            model_name='musetransactionlog',
            index=models.Index(fields=['created_at'], name='muse_txlog_created_idx'),
        ),
        migrations.AddIndex(
            model_name='musetransactionlog',
            index=models.Index(fields=['status'], name='muse_txlog_status_idx'),
        ),
        migrations.AddIndex(
            model_name='musetransactionlog',
            index=models.Index(fields=['transaction_type'], name='muse_txlog_type_idx'),
        ),
    ]
