use sea_orm_migration::prelude::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20251217_000001_create_chunked_upload_session_table"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ChunkedUploadSession::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ChunkedUploadSession::SessionId)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ChunkedUploadSession::CacheId).big_integer().not_null())
                    .col(ColumnDef::new(ChunkedUploadSession::NarInfo).text().not_null())
                    .col(ColumnDef::new(ChunkedUploadSession::TotalChunks).integer().not_null())
                    .col(ColumnDef::new(ChunkedUploadSession::ReceivedChunks).text().not_null())
                    .col(ColumnDef::new(ChunkedUploadSession::CdcChunks).text().not_null())
                    .col(ColumnDef::new(ChunkedUploadSession::TotalBytes).big_integer().not_null())
                    .col(ColumnDef::new(ChunkedUploadSession::NextExpectedChunk).integer().not_null())
                    .col(
                        ColumnDef::new(ChunkedUploadSession::CreatedAt)
                            .timestamp()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ChunkedUploadSession::LastActivity)
                            .timestamp()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx-chunked_upload_session-last_activity")
                    .table(ChunkedUploadSession::Table)
                    .col(ChunkedUploadSession::LastActivity)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ChunkedUploadSession::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
enum ChunkedUploadSession {
    Table,
    SessionId,
    CacheId,
    NarInfo,
    TotalChunks,
    ReceivedChunks,
    CdcChunks,
    TotalBytes,
    NextExpectedChunk,
    CreatedAt,
    LastActivity,
}
