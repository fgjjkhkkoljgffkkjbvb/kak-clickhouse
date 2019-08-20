<?php

namespace kak\clickhouse;

use kak\clickhouse\exceptions\ResponseException;
use kak\clickhouse\exceptions\ResponseHeadersException;

class MysqlCompatibleCommand extends Command
{
    public const HEADER_SUMMARY = 'x-clickhouse-summary';

    /**
     * @param bool $prepare
     * @return array|int|mixed
     * @throws ResponseException
     * @throws \yii\db\Exception
     */
    public function execute($prepare = false)
    {
        $response = parent::execute($prepare);

        if ($prepare) {
            return $response;
        }

        if (!$response) {
            throw new ResponseException('Empty response');
        }

        $summary = $response->headers[static::HEADER_SUMMARY] ?? false;

        if (empty($summary)) {
            throw new ResponseHeadersException(
                sprintf('Can\'t get %s header from response', static::HEADER_SUMMARY)
            );
        }

        $summaryParsed = @json_decode($summary, false);

        if (!$summaryParsed) {
            throw new ResponseHeadersException(
                sprintf('Can\'t parse %s header', static::HEADER_SUMMARY)
            );
        }

        return $summaryParsed->written_rows ?? 0;
    }
}
