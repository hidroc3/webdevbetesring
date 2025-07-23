import { Test, TestingModule } from '@nestjs/testing';
import { AwsLogsService } from './aws-logs.service';

describe('AwsLogsService', () => {
  let service: AwsLogsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AwsLogsService],
    }).compile();

    service = module.get<AwsLogsService>(AwsLogsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
