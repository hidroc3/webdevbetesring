import { Test, TestingModule } from '@nestjs/testing';
import { AwlrLogsService } from './awlr-logs.service';

describe('AwlrLogsService', () => {
  let service: AwlrLogsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AwlrLogsService],
    }).compile();

    service = module.get<AwlrLogsService>(AwlrLogsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
