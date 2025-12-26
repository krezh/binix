use sea_orm_migration::prelude::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20251219_000001_drop_next_expected_chunk"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(ChunkedUploadSession::Table)
                    .drop_column(ChunkedUploadSession::NextExpectedChunk)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(ChunkedUploadSession::Table)
                    .add_column(
                        ColumnDef::new(ChunkedUploadSession::NextExpectedChunk)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await
    }
}

#[derive(Iden)]
enum ChunkedUploadSession {
    Table,
    NextExpectedChunk,
}
